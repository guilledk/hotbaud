# The MIT License (MIT)
#
# Copyright © 2025 Guillermo Rodriguez & Tyler Goodlet
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the “Software”), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
'''
A *Unix* style "pipe/pipeline" of callables, optional fan in/out to multi
processes.

Meant for:

    - Simple "throw away pipelines"
    - Testing
    - Simple benchmarks

'''

from __future__ import annotations

import os
import enum
import logging

from typing import Any, Awaitable, Callable, Generator, Protocol, Sequence
from pathlib import Path
from functools import partial
from dataclasses import dataclass

import anyio
import msgspec

from hotbaud.eventfd import (
    EFD_SEMAPHORE,
    EFDSyncMethods,
    default_sync_method,
    ll_open_eventfd,
)
from hotbaud.experimental.event import Event
from hotbaud.types import SharedMemory
from hotbaud.memchan import (
    MCToken,
    alloc_memory_channel,
)

from hotbaud._utils import make_partial, oom_self_reaper
from hotbaud._fdshare import fdshare_server_task, open_fd_share_socket

from hotbaud.experimental._worker import run_in_worker


log = logging.getLogger(__name__)


class ConnectionStrategy(enum.StrEnum):
    '''
    How messages flow from *this* stage to the next.

    '''

    ONE_TO_ONE = 'one_to_one'
    ONE_TO_MANY = 'one_to_many'
    MANY_TO_ONE = 'many_to_one'

    def validate(self, up_size: int, down_size: int) -> None:
        match self:
            case ConnectionStrategy.ONE_TO_ONE:
                if up_size != down_size:
                    raise ValueError(
                        f'ONE_TO_ONE expects equal worker counts (up={up_size}, down={down_size})'
                    )
            case ConnectionStrategy.ONE_TO_MANY:
                if up_size != 1 or down_size < 2:
                    raise ValueError(
                        'ONE_TO_MANY requires 1 upstream worker and ≥2 downstream workers'
                    )
            case ConnectionStrategy.MANY_TO_ONE:
                if up_size < 2 or down_size != 1:
                    raise ValueError(
                        'MANY_TO_ONE requires ≥2 upstream workers and exactly 1 downstream worker'
                    )


@dataclass(slots=True, frozen=True)
class StageDef:
    '''
    A homogenous pool of *identical* workers (same callable).

    '''

    id: str
    func: partial
    exit_fd: Event | None
    size: int = 1
    async_lib: str | None = None

    # channel aliases scoped to this stage
    inputs: Sequence[str] = ()
    in_size: int = 64 * 1024
    outputs: Sequence[str] = ()
    out_size: int = 64 * 1024

    # how to connect *from this* stage *to the next* (default ONE_TO_ONE)
    strategy: ConnectionStrategy = ConnectionStrategy.ONE_TO_ONE

    def iter_worker_ids(self) -> Generator[str, None, None]:
        return (f'{self.id}[{i}]' for i in range(self.size))


@dataclass(slots=True)
class ChannelDef:
    '''
    Metadata about one logical IPC memory channel.

    '''

    name: str
    shm: SharedMemory | None = None
    token: MCToken | None = None
    buf_size: int = 64 * 1024  # 64kb


class PipelineBuilder:
    '''
    Fluent, declarative builder for process pipelines.

    '''

    def __init__(
        self,
        pipe_id: str,
        *,
        global_config: dict | msgspec.Struct | None = None,
        log_setup: Callable[[], None] | None = None,
        sync_backend: EFDSyncMethods = default_sync_method,
        share_path: Path | str = Path('.hotbaud/share'),
    ):
        self.pipe_id = pipe_id
        self._config = global_config
        self._log_setup = log_setup
        self._sync_backend: EFDSyncMethods = sync_backend
        self._stages: list[StageDef] = []
        self._channels: dict[str, ChannelDef] = {}

        self._socket_dir = Path(share_path) / str(os.getpid())
        self._socket_dir.mkdir(parents=True)

    def stage(
        self,
        sid: str,
        func: partial | Callable,
        *,
        inputs: str | Sequence[str] | None = None,
        in_size: int = 64 * 1024,
        outputs: str | Sequence[str] | None = None,
        out_size: int = 64 * 1024,
        async_lib: str | None = None,
        size: int = 1,
        strategy: ConnectionStrategy | str = ConnectionStrategy.ONE_TO_ONE,
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
            exit_fd=None,
            size=size,
            inputs=(inputs,)
            if isinstance(inputs, str)
            else tuple(inputs or ()),
            in_size=in_size,
            outputs=(outputs,)
            if isinstance(outputs, str)
            else tuple(outputs or ()),
            out_size=out_size,
            async_lib=async_lib,
            strategy=strat,
        )
        self._stages.append(sdef)

        for ch in sdef.inputs:
            self._channels.setdefault(ch, ChannelDef(name=ch, buf_size=in_size))

        for ch in sdef.outputs:
            self._channels.setdefault(
                ch, ChannelDef(name=ch, buf_size=out_size)
            )

        return self

    def one_to_many(
        self,
        sid: str,
        func: partial,
        *,
        input: str | None = None,
        in_size: int = 64 * 1024,
        outputs: str | Sequence[str] | None = None,
        out_size: int = 64 * 1024,
        async_lib: str | None = None,
        size: int = 1,
    ) -> PipelineBuilder:
        '''
        Syntax sugar for a .stage() call with `ONE_TO_MANY` strategy

        '''
        return self.stage(
            sid,
            func,
            inputs=[input] if input else None,
            in_size=in_size,
            outputs=outputs,
            out_size=out_size,
            async_lib=async_lib,
            size=size,
            strategy=ConnectionStrategy.ONE_TO_MANY,
        )

    def many_to_one(
        self,
        sid: str,
        func: partial,
        *,
        inputs: str | Sequence[str] | None = None,
        in_size: int = 64 * 1024,
        output: str | None = None,
        out_size: int = 64 * 1024,
        async_lib: str | None = None,
        size: int = 1,
    ) -> PipelineBuilder:
        '''
        Syntax sugar for a .stage() call with `MANY_TO_ONE` strategy

        '''
        return self.stage(
            sid,
            func,
            inputs=inputs,
            in_size=in_size,
            outputs=[output] if output else None,
            out_size=out_size,
            async_lib=async_lib,
            size=size,
            strategy=ConnectionStrategy.MANY_TO_ONE,
        )

    def _auto_expand_channels(self) -> None:
        '''
        Convert a single string name into multiple channel definitions made out of
        the prefix + the worker index depending on the topology of the pipeline.

        '''

        def _dup(prefix: str, n: int) -> tuple[str, ...]:
            return tuple(f'{prefix}{i}' for i in range(n))

        new_stages: list[StageDef] = []

        for idx, s in enumerate(self._stages):
            prev = self._stages[idx - 1] if idx else None
            nxt = self._stages[idx + 1] if idx + 1 < len(self._stages) else None

            # figure inputs
            inps: tuple[str, ...] = s.inputs
            if len(inps) == 1 and isinstance(inps[0], str):
                need = (
                    s.size
                    if prev and prev.strategy is ConnectionStrategy.ONE_TO_MANY
                    else (prev.size if prev else s.size)
                )
                if need > 1:
                    inps = _dup(inps[0], need)

            # figure outputs
            outs: tuple[str, ...] = s.outputs
            if len(outs) == 1 and isinstance(outs[0], str):
                need = (
                    nxt.size
                    if s.strategy is ConnectionStrategy.ONE_TO_MANY and nxt
                    else s.size
                )
                if need > 1:
                    outs = _dup(outs[0], need)

            exit_fd = None
            if idx > 0:
                exit_fd = Event(ll_open_eventfd(flags=EFD_SEMAPHORE))

            # build a fresh, *expanded* StageDef
            new_stages.append(
                StageDef(
                    id=s.id,
                    func=s.func,
                    exit_fd=exit_fd,
                    size=s.size,
                    inputs=inps,
                    in_size=s.in_size,
                    outputs=outs,
                    out_size=s.out_size,
                    async_lib=s.async_lib,
                    strategy=s.strategy,
                )
            )

        # replace the list atomically so everything downstream sees the copies
        self._stages = new_stages

        # rebuild the channel registry
        self._channels.clear()
        for st in self._stages:
            for ch in st.inputs:
                self._channels.setdefault(
                    ch, ChannelDef(name=ch, buf_size=st.in_size)
                )
            for ch in st.outputs:
                self._channels.setdefault(
                    ch, ChannelDef(name=ch, buf_size=st.out_size)
                )

    def _validate_topology(self) -> None:
        '''
        Ensure declared stage sizes & strategies fit together.

        '''
        pairs = zip(self._stages, self._stages[1:])
        for upstream, downstream in pairs:
            # special-case: many ONE_TO_ONE workers feeding a single MANY_TO_ONE sink
            if (
                upstream.strategy is ConnectionStrategy.ONE_TO_ONE
                and downstream.strategy is ConnectionStrategy.MANY_TO_ONE
                and downstream.size == 1
            ):
                continue

            upstream.strategy.validate(upstream.size, downstream.size)
            # we could extend validation here (channel compatibility etc.)

    def _alloc_channels(self) -> None:
        '''
        Allocate all the resources needed for the pipeline

        '''
        for cdef in self._channels.values():
            key = f'{self.pipe_id}.{cdef.name}'
            (
                cdef.shm,
                cdef.token,
            ) = alloc_memory_channel(
                key,
                buf_size=cdef.buf_size,
                share_path=str(self._socket_dir / f'{key}.sock'),
                sync_backend=self._sync_backend,
            )

    def build(self) -> 'Pipeline':
        '''
        Materialise shared memory & return a :class:`Pipeline` instance.

        '''
        if not self._stages:
            raise RuntimeError('Pipeline must contain at least one stage')

        self._auto_expand_channels()
        self._validate_topology()
        self._alloc_channels()

        workers: list[PipelineWorker] = []

        for sdef in self._stages:
            # inject, depending on topology, the input and/or output token or
            # tokens into the function's `in_token(s)` & `out_token(s)` keyword
            # arguments
            for idx in range(sdef.size):
                func = partial(sdef.func)  # fresh copy
                kw: dict[str, Any] = {}

                # input side
                if sdef.inputs:
                    in_toks = [self._channels[ch].token for ch in sdef.inputs]
                    if sdef.strategy is ConnectionStrategy.MANY_TO_ONE:
                        kw['in_tokens'] = in_toks  # N -> 1
                    elif (
                        sdef.strategy is ConnectionStrategy.ONE_TO_ONE
                        and len(in_toks) >= sdef.size
                    ):
                        kw['in_token'] = in_toks[idx]  # 1 -> 1
                    else:
                        kw['in_token'] = in_toks[0]  # single input

                # output side
                if sdef.outputs:
                    out_toks = [self._channels[ch].token for ch in sdef.outputs]
                    if sdef.strategy is ConnectionStrategy.ONE_TO_MANY:
                        kw['out_tokens'] = out_toks  # 1 -> N
                    elif (
                        sdef.strategy is ConnectionStrategy.ONE_TO_ONE
                        and len(out_toks) >= sdef.size
                    ):
                        kw['out_token'] = out_toks[idx]  # 1 -> 1
                    else:
                        kw['out_token'] = out_toks[0]  # single output

                func.keywords.update(kw)

                wid = f'{sdef.id}[{idx}]'
                workers.append(PipelineWorker(id=wid, func=func, stage=sdef))

        return Pipeline(
            self.pipe_id,
            self._stages,
            workers,
            self._channels,
            self._config,
            self._log_setup,
        )


class WorkerSpawnFn(Protocol):
    def __call__(
        self,
        worker_id: str,
        task: partial,
        exit_fd: int | None,
        *,
        async_lib: str | None,
        config: dict | msgspec.Struct | None,
        log_setup: Callable[[], None] | None,
        env: dict[str, str] | None,
    ) -> Awaitable[Any]: ...


@dataclass(slots=True)
class PipelineWorker:
    id: str
    func: partial
    stage: StageDef


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
        global_config: dict | msgspec.Struct | None,
        log_setup: Callable[[], None] | None,
    ) -> None:
        self.pipe_id = pipe_id
        self.stages = stages
        self.workers = workers
        self.channels = channels
        self.global_config = global_config
        self.log_setup = log_setup

    async def run_stage(
        self,
        stage: StageDef,
        *,
        spawn_fn: WorkerSpawnFn = run_in_worker,
        task_status=anyio.TASK_STATUS_IGNORED,
    ) -> None:
        ''' '''
        idx = self.stages.index(stage)

        # find next stage's exit_event
        next_stage = None
        exit_event: Event | None = None
        if idx < len(self.stages) - 1:
            next_stage = self.stages[idx + 1]
            exit_event = next_stage.exit_fd

        # gather this stage's workers
        workers = tuple((w for w in self.workers if w.stage == stage))

        async with anyio.create_task_group() as tg:
            for w in workers:
                tg.start_soon(
                    partial(
                        spawn_fn,
                        w.id,
                        w.func,
                        exit_event.fd if exit_event else None,
                        async_lib=w.stage.async_lib,
                        config=self.global_config,
                        log_setup=self.log_setup,
                        env=None,
                    )
                )

            log.info(
                f'stage {stage.id} {idx} started with {len(workers)} workers'
            )
            task_status.started()

        # first stage wont do any exit sets
        if stage.exit_fd:
            try:
                stage.exit_fd.set()
                log.info(f'stage {stage.id} set exit')

            except Exception as e:
                log.warning(
                    f'while setting stage {stage.id} (index: {idx}) exit event',
                    exc_info=e,
                )

    async def run(
        self,
        *,
        spawn_fn: WorkerSpawnFn = run_in_worker,
        oom_reap_pct: float = 0.9,
    ) -> None:
        '''
        Long running pipeline task.

        Start all fdshare tasks for channels that need it, then spawn stages

        '''
        with oom_self_reaper(kill_at_pct=oom_reap_pct):
            async with anyio.create_task_group() as tg:
                # channels with .token.share_path need a task spawned to open the
                # socket and pass the fds to clients
                for c in self.channels.values():
                    assert c.token, 'Expected {c.name} to have token at this point'
                    if c.token.share_path:
                        tg.start_soon(
                            fdshare_server_task,
                            Path(c.token.share_path),
                            c.token.fds,
                        )

                # use an inner tg in order to have separate task scopes for
                # stage tasks & fd share tasks
                async with anyio.create_task_group() as stage_group:
                    # spawn all workers
                    for s in self.stages:
                        await stage_group.start(
                            partial(self.run_stage, s, spawn_fn=spawn_fn)
                        )

                    log.info(
                        f'pipeline {self.pipe_id} started with '
                        f'{len(self.workers)} workers across '
                        f'{len(self.stages)} stages'
                    )

                    # will now block on this scope until all stage tasks return

                # cancel fd share tasks
                tg.cancel_scope.cancel()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        for c in self.channels.values():
            if c.shm is not None:
                c.shm.close()
                c.shm.unlink()
