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
IPC MemoryChannel implementation

Trying to follow trio channel semantics as close as posible.

(Linux only atm)

There are two main components:

- `multiprocessing.shared_memory.SharedMemory`
- `hotbaud.EventFD`

Which in reality just are wrappers to `mmap(2)` (posix/sysV/BSD) and
`eventfd(2)` (linux).

###

TODO: generalize overrun semantics & make it configurable:

    - hotbaud.Event() to abstract `eventfd(2)`, `kqueue(2)` & `EventObjects`

    - abstract the MemoryChannel more to allow for different models of shared
    mem access syncronization, using the different primitives / algos (imagine
    the benchmarks!)

    - wizard level tech, User-space Interrupts:

    > There is a ~9x or higher performance improvement using User IPI over other
      IPC mechanisms for event signaling.

    > https://lwn.net/ml/linux-kernel/20210913200132.3396598-1-sohil.mehta@intel.com/

###

'''

from __future__ import annotations

from pathlib import Path
import sys
import struct
import logging

from typing import Any, AsyncGenerator
from contextlib import asynccontextmanager as acm, suppress
from multiprocessing.shared_memory import SharedMemory

import anyio
from anyio import get_cancelled_exc_class
from anyio.abc import AsyncBackend
from anyio._core._eventloop import get_async_backend

from msgspec.msgpack import (
    Encoder,
    Decoder,
)

from hotbaud._fdshare import maybe_open_fd_share_socket
from hotbaud.types import Buffer
from hotbaud.errors import HotbaudInternalError as InternalError
from hotbaud.eventfd import (
    EFD_NONBLOCK,
    EFDSyncMethods,
    default_sync_method,
    ll_open_eventfd,
    EFDReadCancelled,
    EventFD,
)
from hotbaud._utils import MessageStruct


if sys.version_info.minor < 13:
    from hotbaud._utils import disable_resource_tracker

    disable_resource_tracker()


log = logging.getLogger(__name__)


_DEFAULT_RB_SIZE = 10 * 1024


class MCToken(MessageStruct, frozen=True):
    '''
    MemoryChannel token contains necesary info to open resources of a channel

    '''

    shm_name: str

    write_eventfd: int  # used to signal writer ptr advance
    read_eventfd: int  # used to signal reader ptr advance
    eof_eventfd: int  # used to signal writer closed

    buf_size: int  # size in bytes of underlying shared memory Buffer
    sync_backend: EFDSyncMethods  # method of fd read sync: epoll or thread
    share_path: str | None = None  # where to request the fds

    @property
    def fds(self) -> tuple[int, int, int]:
        return (self.write_eventfd, self.read_eventfd, self.eof_eventfd)


def alloc_memory_channel(
    shm_name: str,
    *,
    buf_size: int = _DEFAULT_RB_SIZE,
    share_path: str | Path | None = None,
    sync_backend: EFDSyncMethods = default_sync_method,
) -> tuple[SharedMemory, MCToken]:
    '''
    Allocate OS resources for a memory channel.

    '''
    extra_kwargs = {}

    if sys.version_info.minor == 13:
        extra_kwargs['track'] = False

    shm = SharedMemory(
        name=shm_name, size=buf_size, create=True, **extra_kwargs
    )

    eventfd_flags = 0
    if sync_backend == 'epoll':
        eventfd_flags = EFD_NONBLOCK

    token = MCToken(
        shm_name=shm_name,
        write_eventfd=ll_open_eventfd(flags=eventfd_flags),
        read_eventfd=ll_open_eventfd(flags=eventfd_flags),
        eof_eventfd=ll_open_eventfd(flags=eventfd_flags),
        buf_size=buf_size,
        share_path=str(share_path) if share_path else None,
        sync_backend=sync_backend,
    )

    return shm, token


@acm
async def open_memory_channel(
    shm_name: str,
    *,
    buf_size: int = _DEFAULT_RB_SIZE,
    share_path: str | Path | None = None,
    sync_backend: EFDSyncMethods = default_sync_method,
) -> AsyncGenerator[MCToken]:
    '''
    Handle resources for a mem-chan (shm, eventfd, share_sock), yield `MCToken`
    to be used with `attach_to_memory_sender` and `attach_to_memory_receiver`,
    post yield maybe unshare fds and unlink shared memory

    '''
    shm: SharedMemory | None = None
    token: MCToken | None = None
    try:
        shm, token = alloc_memory_channel(
            shm_name,
            buf_size=buf_size,
            share_path=share_path,
            sync_backend=sync_backend,
        )
        async with maybe_open_fd_share_socket(token.share_path, token.fds):
            yield token

    finally:
        if shm:
            with suppress(Exception):
                shm.unlink()


'''
IPC MemoryChannel

`eventfd(2)` is used for read/write pointer sync, to signal writes to
the reader and end of stream.

In order to guarantee full messages are received, all bytes
sent by `MemorySendChannel` are preceded with a 4 byte header
which decodes into a uint32 indicating the actual size of the
next full payload.

'''


class MemorySendChannel:
    '''
    Memory channel sender side implementation

    Do not use directly! manage with `attach_to_memory_sender`
    after having opened a mem-chan context with `open_memory_channel`.

    '''

    def __init__(
        self,
        token: MCToken,
        async_backend: type[AsyncBackend],
        cancel_exc: type[BaseException],
        cleanup: bool = False,
        encoder: Encoder | None = None,
    ):
        self._token = MCToken.from_msg(token)
        self._cancel_exc = cancel_exc

        # mem-chan os resources
        self._shm: SharedMemory | None = None
        self._write_event = EventFD(
            fd=self._token.write_eventfd,
            omode='w',
            sync_backend=token.sync_backend,
            async_backend=async_backend,
        )
        self._read_event = EventFD(
            fd=self._token.read_eventfd,
            omode='r',
            sync_backend=token.sync_backend,
            async_backend=async_backend,
        )
        self._eof_event = EventFD(
            fd=self._token.eof_eventfd,
            omode='w',
            sync_backend=token.sync_backend,
            async_backend=async_backend,
        )

        # current write pointer
        self._wptr: int = 0
        # current read pointer
        self._rptr: int = 0

        # close shm & fds on exit?
        self._cleanup: bool = cleanup

        self._enc: Encoder | None = encoder

        # have we closed this mem-chan?
        # set to `False` on `.open()`
        self._is_closed: bool = True

        # ensure no concurrent `.send_all()` calls
        self._send_all_lock = anyio.Lock()

        # ensure no concurrent `.send()` calls
        self._send_lock = anyio.Lock()

        # ensure no concurrent `.flush()` calls
        self._flush_lock = anyio.Lock()

    @property
    def closed(self) -> bool:
        return self._is_closed

    @property
    def name(self) -> str:
        if not self._shm:
            raise ValueError('shared memory not initialized yet!')
        return self._shm.name

    @property
    def size(self) -> int:
        return self._token.buf_size

    @property
    def write_ptr(self) -> int:
        return self._wptr

    @property
    def read_ptr(self) -> int:
        return self._rptr

    @property
    def write_fd(self) -> int:
        return self._write_event.fd

    @property
    def read_fd(self) -> int:
        return self._read_event.fd

    async def _wait_reader(self) -> int:
        delta = await self._read_event.read()
        self._rptr += delta
        return delta

    async def send_all(self, data: Buffer):
        if self.closed:
            raise anyio.ClosedResourceError

        if self._send_all_lock.locked():
            raise anyio.BusyResourceError('send_all already in progress')

        async with self._send_all_lock:
            mv = memoryview(data)
            off = 0
            mv_len = len(mv)
            while off < mv_len:
                used = self._wptr - self._rptr
                free = self.size - used
                if free == 0:
                    await self._wait_reader()
                    continue

                pos = self._wptr % self.size
                n = min(mv_len - off, free, self.size - pos)
                self._shm.buf[pos : pos + n] = mv[off : off + n]
                self._write_event.write(n)
                self._wptr += n
                off += n

    async def send(self, value: Any) -> None:
        if self.closed:
            raise anyio.ClosedResourceError

        if self._send_lock.locked():
            raise anyio.BusyResourceError('send already in progress')

        if isinstance(value, bytes):
            raw_value = value
        else:
            if not self._enc:
                raise TypeError('encoder required for non-bytes payloads')
            raw_value = self._enc.encode(value)

        async with self._send_lock:
            msg: bytes = struct.pack('<I', len(raw_value)) + raw_value
            await self.send_all(msg)

    def open(self):
        try:
            extra_kwargs = {}
            if sys.version_info.minor == 13:
                extra_kwargs['track'] = False

            self._shm = SharedMemory(
                name=self._token.shm_name,
                size=self._token.buf_size,
                create=False,
                **extra_kwargs,
            )
            self._write_event.open()
            self._read_event.open()
            self._eof_event.open()
            self._is_closed = False

        except Exception as e:
            e.add_note(f'while opening sender for {self._token.to_dict()}')
            raise e

    def _close(self):
        # ensure EOF write always wakes up the reader even when no bytes writen
        # thats why we add 1
        self._eof_event.write(self._wptr + 1)

        if self._cleanup:
            self._write_event.close()
            self._read_event.close()
            self._eof_event.close()

            if self._shm is not None:
                self._shm.close()

        self._is_closed = True

    async def aclose(self):
        if self.closed:
            return

        self._close()


class MemoryReceiveChannel:
    '''
    Memory channel receiver side implementation

    Do not use directly! manage with `attach_to_memory_receiver`
    after having opened a mem-chan context with `open_memory_channel`.

    '''

    def __init__(
        self,
        token: MCToken,
        async_backend: type[AsyncBackend],
        cancel_exc: type[BaseException],
        cleanup: bool = False,
        decoder: Decoder | None = None,
    ):
        self._token = MCToken.from_msg(token)
        self._cancel_exc = cancel_exc

        # mem-chan os resources
        self._shm: SharedMemory | None = None
        self._write_event = EventFD(
            fd=self._token.write_eventfd,
            omode='r',
            sync_backend=token.sync_backend,
            async_backend=async_backend,
        )
        self._read_event = EventFD(
            fd=self._token.read_eventfd,
            omode='w',
            sync_backend=token.sync_backend,
            async_backend=async_backend,
        )
        self._eof_event = EventFD(
            fd=self._token.eof_eventfd,
            omode='r',
            sync_backend=token.sync_backend,
            async_backend=async_backend,
        )

        # current write ptr
        self._wptr: int = 0

        # current read ptr
        self._rptr: int = 0

        # end ptr is used when EOF is signaled; contains maximum
        # readable position in absolute pointer space
        self._end_ptr: int = -1

        # close shm & fds on exit?
        self._cleanup: bool = cleanup

        # have we closed this mem-chan?
        # set to `False` on `.open()`
        self._is_closed: bool = True

        self._dec: Decoder | None = decoder

        # ensure no concurrent `.receive_some()` calls
        self._receive_some_lock = anyio.Lock()

        # ensure no concurrent `.receive_exactly()` calls
        self._receive_exactly_lock = anyio.Lock()

        # ensure no concurrent `.receive()` calls
        self._receive_lock = anyio.Lock()

    @property
    def closed(self) -> bool:
        return self._is_closed

    @property
    def name(self) -> str:
        if not self._shm:
            raise ValueError('shared memory not initialized yet!')
        return self._shm.name

    @property
    def size(self) -> int:
        return self._token.buf_size

    @property
    def write_ptr(self) -> int:
        return self._wptr

    @property
    def read_ptr(self) -> int:
        return self._rptr

    @property
    def write_fd(self) -> int:
        return self._write_event.fd

    @property
    def read_fd(self) -> int:
        return self._read_event.fd

    @property
    def eof_was_signaled(self) -> bool:
        return self._end_ptr != -1

    async def _eof_monitor_task(self):
        '''
        Long running EOF event monitor, automatically run in bg by
        `attach_to_memory_receiver` context manager, if EOF event
        is set its value will be the end pointer (highest valid
        index to be read from buf, after setting the `self._end_ptr`
        we close the write event which should cancel any blocked
        `self._write_event.read()`s on it.

        '''
        try:
            v = await self._eof_event.read()
            self._end_ptr = max(0, v - 1)

        except EFDReadCancelled:
            ...

        except self._cancel_exc:
            ...

        finally:
            # closing write_event should trigger `EFDReadCancelled`
            # on any pending read
            self._write_event.close()

    def receive_nowait(self, max_bytes: int = _DEFAULT_RB_SIZE) -> bytes:
        '''
        Try to receive any bytes we can without blocking or raise
        `anyio.WouldBlock`.

        Returns b'' when no more bytes can be read (EOF signaled & read all).

        '''
        if max_bytes < 1:
            raise ValueError('max_bytes must be >= 1')

        # available = produced - consumed (seq space)
        highest = max(self._wptr, self._end_ptr)
        delta = highest - self._rptr

        # no more bytes to read
        if delta == 0:
            # EOF and nothing left
            if self.eof_was_signaled:
                return b''

            # signal the need to wait on `write_event`
            raise anyio.WouldBlock

        delta = min(delta, max_bytes)
        pos = self._rptr % self.size
        n = min(delta, self.size - pos)
        segment = bytes(self._shm.buf[pos : pos + n])
        self._rptr += n
        self._read_event.write(n)

        return segment

    async def receive_some(self, max_bytes: int = _DEFAULT_RB_SIZE) -> bytes:
        '''
        Receive up to `max_bytes`, if no `max_bytes` is provided
        a reasonable default is used.

        Can return < max_bytes.

        '''
        if self.closed:
            raise anyio.ClosedResourceError

        if self._receive_some_lock.locked():
            raise anyio.BusyResourceError('receive_some in progress')

        async with self._receive_some_lock:
            while True:
                try:
                    return self.receive_nowait(max_bytes=max_bytes)

                except anyio.WouldBlock as e:
                    if self.eof_was_signaled:
                        raise InternalError(
                            'eof set but receive_nowait raised WouldBlock'
                        ) from e

                    try:
                        delta = await self._write_event.read()
                        self._wptr += delta

                    except (
                        EFDReadCancelled,
                        self._cancel_exc,
                        anyio.BrokenResourceError,
                    ):
                        try:
                            return self.receive_nowait(max_bytes=max_bytes)

                        except anyio.WouldBlock:
                            return b''

    async def receive_exactly(self, num_bytes: int) -> bytes:
        '''
        Fetch bytes until we read exactly `num_bytes` or EOC.

        '''
        if self.closed:
            raise anyio.ClosedResourceError

        if self._receive_exactly_lock.locked():
            raise anyio.BusyResourceError('receive-exactly')

        async with self._receive_exactly_lock:
            buf = bytearray()

            while len(buf) < num_bytes:
                remaining = num_bytes - len(buf)
                new_bytes = await self.receive_some(max_bytes=remaining)
                if new_bytes == b'':
                    break
                buf.extend(new_bytes)

            if len(buf) != num_bytes:
                raise anyio.EndOfStream

            return bytes(buf)

    async def receive(self, raw: bool = False) -> Any:
        '''
        Receive a complete payload or raise EOC

        '''
        if self.closed:
            raise anyio.ClosedResourceError

        if self._receive_lock.locked():
            raise anyio.BusyResourceError('receive')

        async with self._receive_lock:
            header: bytes = await self.receive_exactly(4)
            size: int
            (size,) = struct.unpack('<I', header)
            if size == 0:
                raise anyio.EndOfStream

            raw_msg = await self.receive_exactly(size)
            if raw:
                return raw_msg

            return raw_msg if not self._dec else self._dec.decode(raw_msg)

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        try:
            return await self.receive(raw=True)
        except (anyio.EndOfStream, anyio.ClosedResourceError):
            raise StopAsyncIteration

    async def iter_raw_pairs(self) -> AsyncGenerator[tuple[bytes, Any], None]:
        if not self._dec:
            raise RuntimeError('iter_raw_pairs requires decoder')

        while True:
            try:
                raw = await self.receive(raw=True)
                yield raw, self._dec.decode(raw)

            except anyio.EndOfStream:
                break

    def open(self):
        try:
            extra_kwargs = {}
            if sys.version_info.minor == 13:
                extra_kwargs['track'] = False

            self._shm = SharedMemory(
                name=self._token.shm_name,
                size=self._token.buf_size,
                create=False,
                **extra_kwargs,
            )
            self._write_event.open()
            self._read_event.open()
            self._eof_event.open()
            self._is_closed = False

        except Exception as e:
            e.add_note(f'while opening receiver for {self._token.to_dict()}')
            raise e

    def close(self):
        if self._cleanup:
            self._write_event.close()
            self._read_event.close()
            self._eof_event.close()

            if self._shm is not None:
                self._shm.close()

        self._is_closed = True

    async def aclose(self):
        if self.closed:
            return

        self.close()


async def maybe_negotiate_token(token: MCToken) -> MCToken:
    if not token.share_path:
        return token

    async with await anyio.connect_unix(token.share_path) as sock:
        _, fds = await sock.receive_fds(0, len(token.fds))
        write_fd, read_fd, eof_fd = fds
        return MCToken(
            shm_name=token.shm_name,
            write_eventfd=write_fd,
            read_eventfd=read_fd,
            eof_eventfd=eof_fd,
            buf_size=token.buf_size,
            sync_backend=token.sync_backend,
            share_path=token.share_path,
        )


@acm
async def attach_to_memory_receiver(
    token: MCToken | dict,
    *,
    cleanup: bool = False,
    decoder: Decoder | None = None,
) -> AsyncGenerator[MemoryReceiveChannel, None]:
    '''
    Attach a `MemoryReceiveChannel` from a previously opened
    MCToken.

    Launches `receiver._eof_monitor_task` in a `anyio.TaskGroup`.

    '''
    token = MCToken.from_msg(token)
    token = await maybe_negotiate_token(token)

    async with anyio.create_task_group() as g:
        receiver = MemoryReceiveChannel(
            token,
            async_backend=get_async_backend(),
            cancel_exc=get_cancelled_exc_class(),
            cleanup=cleanup,
            decoder=decoder,
        )
        receiver.open()
        g.start_soon(receiver._eof_monitor_task)
        yield receiver
        g.cancel_scope.cancel()
        receiver.close()


@acm
async def attach_to_memory_sender(
    token: MCToken | dict,
    *,
    cleanup: bool = False,
    encoder: Encoder | None = None,
) -> AsyncGenerator[MemorySendChannel, None]:
    '''
    Attach a `MemorySendChannel` from a previously opened
    MCToken.

    '''
    token = MCToken.from_msg(token)
    token = await maybe_negotiate_token(token)

    sender = MemorySendChannel(
        token,
        async_backend=get_async_backend(),
        cancel_exc=get_cancelled_exc_class(),
        cleanup=cleanup,
        encoder=encoder,
    )
    sender.open()
    yield sender
    await sender.aclose()


class MemoryChannel:
    '''
    Combine `MemorySendChannel` and `MemoryReceiveChannel`
    in order to expose the bidirectional  API.

    '''

    def __init__(
        self, sender: MemorySendChannel, receiver: MemoryReceiveChannel
    ):
        self._sender = sender
        self._receiver = receiver

    async def send_all(self, value: bytes) -> None:
        await self._sender.send_all(value)

    async def send(self, value: bytes) -> None:
        await self._sender.send(value)

    def receive_nowait(self, max_bytes: int = _DEFAULT_RB_SIZE) -> bytes:
        return self._receiver.receive_nowait(max_bytes=max_bytes)

    async def receive_some(self, max_bytes: int = _DEFAULT_RB_SIZE) -> bytes:
        return await self._receiver.receive_some(max_bytes=max_bytes)

    async def receive_exactly(self, num_bytes: int) -> bytes:
        return await self._receiver.receive_exactly(num_bytes)

    async def receive(self) -> bytes:
        return await self._receiver.receive()

    async def aclose(self):
        await self._receiver.aclose()
        await self._sender.aclose()


@acm
async def attach_to_memory_channel(
    token_in: MCToken | dict,
    token_out: MCToken | dict,
    *,
    cleanup_in: bool = False,
    cleanup_out: bool = False,
    encoder: Encoder | None = None,
    decoder: Decoder | None = None,
) -> AsyncGenerator[MemoryChannel]:
    '''
    Attach to two previously opened `MCToken`s and return a `MemoryChannel`

    '''
    async with (
        attach_to_memory_receiver(
            token_in,
            cleanup=cleanup_in,
            decoder=decoder,
        ) as receiver,
        attach_to_memory_sender(
            token_out,
            cleanup=cleanup_out,
            encoder=encoder,
        ) as sender,
    ):
        yield MemoryChannel(sender, receiver)
