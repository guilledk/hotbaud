from __future__ import annotations

import enum
from functools import partial
import itertools
import logging
from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any, Awaitable, Callable, Generator, Sequence

import trio
from trio.socket import SocketType
from hotbaud import run_in_worker
from hotbaud.memchan import (
    MCToken,
    alloc_memory_channel,
)
from hotbaud.types import SharedMemory
from hotbaud._utils import make_partial
from hotbaud._fdshare import open_fd_share_socket

log = logging.getLogger(__name__)


#  Enums / data‑classes


class ConnectionStrategy(enum.StrEnum):
    '''
    How messages flow from *this* stage to the next.

    '''

    STRICT = 'strict'
    FAN_OUT = 'fan_out'
    FAN_IN = 'fan_in'

    def validate(self, up_size: int, down_size: int) -> None:
        match self:
            case ConnectionStrategy.STRICT:
                if up_size != down_size:
                    raise ValueError(
                        f'STRICT expects equal worker counts (up={up_size}, down={down_size})'
                    )
            case ConnectionStrategy.FAN_OUT:
                if up_size != 1 or down_size < 2:
                    raise ValueError(
                        'FAN_OUT requires 1 upstream worker and ≥2 downstream workers'
                    )
            case ConnectionStrategy.FAN_IN:
                if up_size < 2 or down_size != 1:
                    raise ValueError(
                        'FAN_IN requires ≥2 upstream workers and exactly 1 downstream worker'
                    )


@dataclass(slots=True, frozen=True)
class StageDef:
    '''
    A homogenous pool of *identical* workers (same callable).

    '''

    id: str
    func: partial
    size: int = 1

    # channel aliases scoped to this stage
    inputs: Sequence[str] = ()
    outputs: Sequence[str] = ()

    # how to connect *from this* stage *to the next* (default STRICT)
    strategy: ConnectionStrategy = ConnectionStrategy.STRICT

    def iter_worker_ids(self) -> Generator[str, None, None]:
        return (f'{self.id}[{i}]' for i in range(self.size))


@dataclass(slots=True)
class ChannelDef:
    '''
    Metadata about one logical ring‑buffer channel.

    '''

    name: str
    shm: SharedMemory | None = None
    share_sock: SocketType | None = None
    token: MCToken | None = None
    buf_size: int = 64 * 1024  # 64kb

    async def fdshare_task(self) -> None:
        if self.share_sock is None:
            raise RuntimeError('Started fdshare_task on a None self.share_sock ?')

        if self.token is None:
            raise RuntimeError('Started fdshare_task on a None self.token ?')

        async with open_fd_share_socket(self.share_sock, self.token.fds):
            await trio.sleep_forever()


class PipelineBuilder:
    '''
    Fluent, declarative builder for process pipelines.

    '''

    def __init__(self, pipe_id: str):
        self.pipe_id = pipe_id
        self._stages: list[StageDef] = []
        self._channels: dict[str, ChannelDef] = {}

        self._socket_dir = Path('.hotbaud/share') / str(os.getpid())
        self._socket_dir.mkdir(parents=True)

    def stage(
        self,
        sid: str,
        func: partial | Callable | Awaitable,
        *,
        inputs: str | Sequence[str] | None = None,
        outputs: str | Sequence[str] | None = None,
        size: int = 1,
        strategy: ConnectionStrategy | str = ConnectionStrategy.STRICT,
    ) -> PipelineBuilder:
        '''
        Add a *stage* (worker pool) to the pipeline.

        '''
        if not outputs and not inputs:
            raise ValueError(
                'Stage must declare at least one input or output channel'
            )

        func = make_partial(func)

        strat = (
            strategy
            if isinstance(strategy, ConnectionStrategy)
            else ConnectionStrategy(strategy)
        )

        sdef = StageDef(
            id=f'{self.pipe_id}.{sid}',
            func=func,
            size=size,
            inputs=(inputs,)
            if isinstance(inputs, str)
            else tuple(inputs or ()),
            outputs=(outputs,)
            if isinstance(outputs, str)
            else tuple(outputs or ()),
            strategy=strat,
        )
        self._stages.append(sdef)

        for ch in itertools.chain(sdef.inputs, sdef.outputs):
            self._channels.setdefault(ch, ChannelDef(
                name=ch,
            ))

        return self

    def fan_out(
        self,
        sid: str,
        func: partial,
        *,
        input: str | None = None,
        outputs: str | Sequence[str] | None = None,
        size: int = 1,
    ) -> PipelineBuilder:
        return self.stage(
            sid,
            func,
            inputs=[input] if input else None,
            outputs=outputs,
            size=size,
            strategy=ConnectionStrategy.FAN_OUT,
        )

    def fan_in(
        self,
        sid: str,
        func: partial,
        *,
        inputs: str | Sequence[str] | None = None,
        output: str | None = None,
        size: int = 1,
    ) -> PipelineBuilder:
        return self.stage(
            sid,
            func,
            inputs=inputs,
            outputs=[output] if output else None,
            size=size,
            strategy=ConnectionStrategy.FAN_IN,
        )

    def _auto_expand_channels(self) -> None:
        def _dup(prefix: str, n: int) -> tuple[str, ...]:
            return tuple(f'{prefix}{i}' for i in range(n))

        new_stages: list[StageDef] = []

        for idx, s in enumerate(self._stages):
            prev = self._stages[idx - 1] if idx else None
            nxt = self._stages[idx + 1] if idx + 1 < len(self._stages) else None

            # -------- figure inputs --------
            inps: tuple[str, ...] = s.inputs
            if len(inps) == 1 and isinstance(inps[0], str):
                need = (
                    s.size
                    if prev and prev.strategy is ConnectionStrategy.FAN_OUT
                    else (prev.size if prev else s.size)
                )
                if need > 1:
                    inps = _dup(inps[0], need)

            # -------- figure outputs --------
            outs: tuple[str, ...] = s.outputs
            if len(outs) == 1 and isinstance(outs[0], str):
                need = (
                    nxt.size
                    if s.strategy is ConnectionStrategy.FAN_OUT and nxt
                    else s.size
                )
                if need > 1:
                    outs = _dup(outs[0], need)

            # build a fresh, *expanded* StageDef
            new_stages.append(
                StageDef(
                    id=s.id,
                    func=s.func,
                    size=s.size,
                    inputs=inps,
                    outputs=outs,
                    strategy=s.strategy,
                )
            )

        # replace the list atomically so everything downstream sees the copies
        self._stages = new_stages

        # rebuild the channel registry
        self._channels.clear()
        for st in self._stages:
            for ch in (*st.inputs, *st.outputs):
                self._channels.setdefault(ch, ChannelDef(name=ch))

    def _validate_topology(self) -> None:
        '''Ensure declared stage sizes & strategies fit together.'''
        pairs = zip(self._stages, self._stages[1:])
        for upstream, downstream in pairs:
            # Special-case: many STRICT workers feeding a single FAN_IN sink
            if (
                upstream.strategy is ConnectionStrategy.STRICT
                and downstream.strategy is ConnectionStrategy.FAN_IN
                and downstream.size == 1
            ):
                continue

            upstream.strategy.validate(upstream.size, downstream.size)
            # we could extend validation here (channel compatibility etc.)

    async def _alloc_channels(self) -> None:
        for cdef in self._channels.values():
            key = f'{self.pipe_id}.{cdef.name}'
            (
                cdef.shm,
                cdef.token,
                cdef.share_sock
            ) = await alloc_memory_channel(
                key,
                buf_size=cdef.buf_size,
                share_path=str(self._socket_dir / f'{key}.sock')
            )

    async def build(self) -> 'Pipeline':
        '''
        Materialise shared memory & return a :class:`Pipeline` instance.

        '''
        if not self._stages:
            raise RuntimeError('Pipeline must contain at least one stage')

        self._auto_expand_channels()
        self._validate_topology()

        await self._alloc_channels()

        workers: list[PipelineWorker] = []

        for sdef in self._stages:
            # Prepare one fully-resolved *callable* per worker instance so that
            # positional/keyword manipulation below does not bleed across
            # workers.
            for idx in range(sdef.size):
                func = partial(sdef.func)  # fresh copy
                kw: dict[str, Any] = {}

                # input side
                if sdef.inputs:
                    in_toks = [self._channels[ch].token for ch in sdef.inputs]
                    if sdef.strategy is ConnectionStrategy.FAN_IN:
                        kw['in_tokens'] = in_toks  # N -> 1
                    elif (
                        sdef.strategy is ConnectionStrategy.STRICT
                        and len(in_toks) >= sdef.size
                    ):
                        kw['in_token'] = in_toks[idx]  # 1 <-> 1
                    else:
                        kw['in_token'] = in_toks[0]  # single input

                # output side
                if sdef.outputs:
                    out_toks = [self._channels[ch].token for ch in sdef.outputs]
                    if sdef.strategy is ConnectionStrategy.FAN_OUT:
                        kw['out_tokens'] = out_toks  # 1 -> N
                    elif (
                        sdef.strategy is ConnectionStrategy.STRICT
                        and len(out_toks) >= sdef.size
                    ):
                        kw['out_token'] = out_toks[idx]  # 1 <-> 1
                    else:
                        kw['out_token'] = out_toks[0]  # single output

                func.keywords.update(kw)

                wid = f'{sdef.id}[{idx}]'
                workers.append(PipelineWorker(id=wid, func=func))

        return Pipeline(self.pipe_id, self._stages, workers, self._channels)


#  Runtime pieces - very small; heavy lifting is done in builder helpers


@dataclass(slots=True)
class PipelineWorker:
    id: str
    func: partial

    async def run(self) -> None:
        await run_in_worker(
            self.id,
            self.func,
            asyncio=False,
            # worker_type='fork',
        )


class Pipeline:
    '''
    Executable pipeline returned by :pymeth:`PipelineBuilder.build`.

    '''

    def __init__(
        self,
        pipe_id: str,
        stages: list[StageDef],
        workers: list[PipelineWorker],
        channels: dict[str, ChannelDef],
    ) -> None:
        self.pipe_id = pipe_id
        self.stages = stages
        self.workers = workers
        self.channels = channels

    def token(self, ch_name: str) -> MCToken:
        '''Expose the *MCToken* for a given channel name.'''
        return self.channels[ch_name].token  # type: ignore[return-value]

    async def run(self) -> None:
        '''Spawn all workers, plus an optional path‑publisher.'''
        async with trio.open_nursery() as nursery:
            for c in self.channels.values():
                assert c.token, 'Expected {c.name} to have token at this point'
                if c.token.share_path:
                    nursery.start_soon(c.fdshare_task)

            for w in self.workers:
                nursery.start_soon(w.run)

            log.info(
                'pipeline %s started with %d workers across %d stages',
                self.pipe_id,
                len(self.workers),
                len(self.stages),
            )

    #  Async context manager - clean up SHM

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        for c in self.channels.values():
            if c.shm is not None:
                c.shm.close()
                c.shm.unlink()
