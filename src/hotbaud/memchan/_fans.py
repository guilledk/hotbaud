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

'''

from __future__ import annotations

from contextlib import AsyncExitStack, asynccontextmanager
import itertools

from heapq import heappush, heappop
from typing import AsyncGenerator, AsyncIterator, Awaitable, Protocol, Sequence

import trio
import msgspec

from msgspec import Raw

from hotbaud._utils import MessageStruct
from hotbaud.types import Buffer

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


class FanOutSendFn(Protocol):
    def __call__(
        self, payload: Buffer, *, broadcast: bool = False
    ) -> Awaitable[None]: ...


@asynccontextmanager
async def attach_fan_out_sender(
    out_tokens: Sequence[MCToken],
    *,
    batch_size: int = 1,
    ordered: bool = False,
) -> AsyncGenerator[FanOutSendFn, None]:
    if not out_tokens:
        raise ValueError('attach_fan_out_sender expects at least one token')

    seq = itertools.count().__next__  # monotonically increasing index
    rr_idx = 0  # round-robin pointer

    async with AsyncExitStack() as stack:
        senders = [
            await stack.enter_async_context(
                attach_to_memory_sender(t, batch_size=batch_size)
            )
            for t in out_tokens
        ]

        async def send(payload: Buffer, broadcast: bool = False) -> None:
            raw = (
                OrderedMsg(index=seq(), msg=payload).encode()
                if ordered
                else payload
            )

            if broadcast:
                async with trio.open_nursery() as n:
                    for sender in senders:
                        n.start_soon(sender.send, raw)

                return

            nonlocal rr_idx
            sender = senders[rr_idx]
            rr_idx = (rr_idx + 1) % len(senders)

            await sender.send(raw)

        yield send


@asynccontextmanager
async def attach_fan_in_receiver(
    in_tokens: Sequence[MCToken],
    *,
    ordered: bool = False,
    start_index: int = 0,
) -> AsyncGenerator[AsyncIterator[tuple[int, bytes]], None]:
    if len(in_tokens) < 2:
        raise ValueError('attach_fan_in_receiver expects ≥2 tokens')

    send, recv = trio.open_memory_channel(0)

    async with trio.open_nursery() as nursery, AsyncExitStack() as stack:
        # open all upstream receivers
        receivers = [
            await stack.enter_async_context(attach_to_memory_receiver(t))
            for t in in_tokens
        ]

        # background pumps - one per upstream channel
        async def _pump(idx: int, rcv):
            try:
                async for msg in rcv:
                    await send.send((idx, msg))
                await send.send((idx, None))  # sentinel

            except trio.ClosedResourceError:
                ...

        for i, r in enumerate(receivers):
            nursery.start_soon(_pump, i, r)

        # ordered-delivery helpers
        next_idx = start_index
        pqueue: list[tuple[int, tuple[int, bytes]]] = []

        def can_pop() -> bool:
            return bool(pqueue) and pqueue[0][0] == next_idx

        async def drain_heap():
            while not can_pop():
                i, raw = await recv.receive()
                om = OrderedMsg.from_bytes(raw)
                heappush(pqueue, (om.index, (i, om.unwrap())))

        async def pop_ordered() -> tuple[int, bytes]:
            nonlocal next_idx
            if not can_pop():
                await drain_heap()
            _, pair = heappop(pqueue)
            next_idx += 1
            return pair

        async def _iter():
            finished = 0
            while True:
                if ordered:
                    idx, payload = await pop_ordered()
                else:
                    idx, payload = await recv.receive()

                if payload is None:  # got sentinel
                    finished += 1
                    if finished == len(receivers):
                        break
                    continue

                yield idx, payload

        yield _iter()  # give caller the async-iterator
        for receiver in receivers:
            await receiver.aclose()
