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
MemoryChannel One-to-Many / Many-to-One helpers

(WIP)

'''
from __future__ import annotations

import itertools

from heapq import heappush, heappop
from typing import AsyncIterator, Sequence

import trio
import msgspec

from msgspec import Raw

from hotbaud._utils import MessageStruct

from ._impl import (
    MCToken,
    attach_to_memory_sender,
    attach_to_memory_receiver,
)


class OrderedMsg(MessageStruct, frozen=True):
    index: int
    msg: Raw

    def unwrap(self, **kwargs) -> msgspec.Struct:
        return msgspec.msgpack.decode(self.msg, **kwargs)


class FanOutSender:
    def __init__(
        self,
        out_tokens: Sequence[MCToken],
        batch_size: int = 1,
        ordered: bool = False,
    ):
        if not out_tokens:
            raise ValueError(
                'FanOutSender expects at least one downstream token'
            )

        self.out_tokens = list(out_tokens)
        self.batch_size = batch_size
        self.ordered = ordered
        # stable, monotonically increasing sequence number (for optional ordering)
        self._seq = itertools.count().__next__
        # round‑robin pointer
        self._rr_idx = 0

    async def __aenter__(self):
        cmgrs = [
            attach_to_memory_sender(tok, batch_size=self.batch_size)
            for tok in self.out_tokens
        ]
        # lazily enter the child contexts (order preserved)
        # keep references for __aexit__
        self._sender_cmgrs = cmgrs
        self._senders = [await cm.__aenter__() for cm in cmgrs]
        return self

    async def __aexit__(self, exc_type, exc, tb):
        # flush + close all children *in reverse order* to mirror stack unwinding
        for cm in self._sender_cmgrs[::-1]:
            await cm.__aexit__(exc_type, exc, tb)

    async def send(self, payload: bytes | bytearray | memoryview) -> None:
        '''
        Send *payload* to the next downstream receiver.

        '''
        sender = self._senders[self._rr_idx]
        self._rr_idx = (self._rr_idx + 1) % len(self._senders)

        raw = payload
        if self.ordered:
            raw = OrderedMsg(index=self._seq(), msg=payload).encode()

        # Let the underlying channel deal with flow‑control / back‑pressure
        await sender.send(raw)


class FanInReceiver:
    def __init__(
        self,
        in_tokens: Sequence[MCToken],
        ordered: bool = False,
        start_index: int = 0,
    ):
        if len(in_tokens) < 2:
            raise ValueError('FanInReceiver expects at least 2 upstream tokens')

        self.in_tokens = list(in_tokens)
        self.ordered = ordered

        self._send, self._recv = trio.open_memory_channel[tuple[int, bytes]](
            max_buffer_size=0
        )

        # only used when ordered == True
        self._next_index = start_index
        self._pqueue = []

    def _can_pop_next(self) -> bool:
        return len(self._pqueue) > 0 and self._pqueue[0][0] == self._next_index

    async def _drain_to_heap(self):
        while not self._can_pop_next():
            idx, msg = await self._recv.receive()
            msg = OrderedMsg.from_bytes(msg)
            heappush(self._pqueue, (msg.index, (idx, msg.unwrap())))

    def _pop_next(self) -> tuple[int, bytes]:
        _, msg = heappop(self._pqueue)
        self._next_index += 1
        return msg

    async def __aenter__(self):
        cmgrs = [attach_to_memory_receiver(tok) for tok in self.in_tokens]
        self._recv_cmgrs = cmgrs
        self._receivers = [await cm.__aenter__() for cm in cmgrs]  # type: ignore[misc]
        return self  # acts as async iterator

    async def __aexit__(self, exc_type, exc, tb):
        for cm in self._recv_cmgrs[::-1]:
            await cm.__aexit__(exc_type, exc, tb)  # type: ignore[misc]

    def __aiter__(
        self,
    ) -> AsyncIterator[tuple[int, bytes]]:
        return self._run()

    async def _receive_ordered(self) -> tuple[int, bytes]:
        if self._can_pop_next():
            return self._pop_next()

        await self._drain_to_heap()
        return self._pop_next()

    async def _receive(self) -> tuple[int, bytes]:
        return await self._recv.receive()

    async def _run(self):
        '''
        Background task that concurrently receives from *all* senders.

        '''
        recv_fn = self._receive if not self.ordered else self._receive_ordered
        async with trio.open_nursery() as nursery:

            async def _pump(idx: int, receiver):
                async for msg in receiver:
                    await self._send.send((idx, msg))
                await self._send.send((idx, None))  # sentinel

            # Start one task per upstream receiver
            for i, r in enumerate(self._receivers):
                nursery.start_soon(_pump, i, r)

            finished = 0
            while True:
                idx, payload = await recv_fn()
                if payload is None:  # got sentinel from _pump()
                    finished += 1
                    if finished == len(self._receivers):
                        break
                else:
                    yield idx, payload
