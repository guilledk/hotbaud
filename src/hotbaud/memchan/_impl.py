'''
IPC MemoryChannel implementation

'''
from __future__ import annotations
import os
import struct
import logging
from typing import (
    AsyncGenerator,
    TypeVar,
)
from contextlib import (
    asynccontextmanager as acm,
    suppress
)
from multiprocessing.shared_memory import SharedMemory

import trio
from trio import socket
from msgspec.msgpack import (
    Encoder,
    Decoder,
)

from hotbaud.types import Buffer
from hotbaud.eventfd import (
    open_eventfd,
    EFDReadCancelled,
    EventFD
)
from hotbaud._utils import InternalError, MessageStruct, disable_resource_tracker
from hotbaud._fdshare import maybe_open_fd_share_socket, recv_fds


log = logging.getLogger(__name__)


disable_resource_tracker()

_DEFAULT_RB_SIZE = 10 * 1024


class MCToken(MessageStruct, frozen=True):
    '''
    MemoryChannel token contains necesary info to open resources of a channel

    '''
    shm_name: str

    write_eventfd: int  # used to signal writer ptr advance
    wrap_eventfd: int  # used to signal reader ready after wrap around
    eof_eventfd: int  # used to signal writer closed

    buf_size: int  # size in bytes of underlying shared memory buffer

    share_path: str | None

    @property
    def fds(self) -> tuple[int, int, int]:
        return (
            self.write_eventfd,
            self.wrap_eventfd,
            self.eof_eventfd
        )


async def alloc_memory_channel(
    shm_name: str,
    *,
    buf_size: int = _DEFAULT_RB_SIZE,
    share_path: str | None = None
) -> tuple[SharedMemory, MCToken, socket.SocketType | None]:
    '''
    Allocate OS resources for a memory channel.

    '''
    shm = SharedMemory(
        name=shm_name,
        size=buf_size,
        create=True
    )
    token = MCToken(
        shm_name=shm_name,
        write_eventfd=open_eventfd(),
        wrap_eventfd=open_eventfd(),
        eof_eventfd=open_eventfd(),
        buf_size=buf_size,
        share_path=share_path
    )
    share_sock = None
    if share_path:
        share_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        await share_sock.bind(share_path)
        share_sock.listen(1)

    return shm, token, share_sock


@acm
async def open_memory_channel(
    shm_name: str,
    *,
    buf_size: int = _DEFAULT_RB_SIZE,
    share_path: str | None = None
) -> AsyncGenerator[MCToken]:
    '''
    Handle resources for a mem-chan (shm, eventfd, share_sock), yield `MCToken`
    to be used with `attach_to_memory_sender` and `attach_to_memory_receiver`,
    post yield maybe unshare fds and unlink shared memory

    '''
    shm: SharedMemory | None = None
    token: MCToken | None = None
    sh_sock: socket.SocketType | None = None
    try:
        shm, token, sh_sock = await alloc_memory_channel(
            shm_name,
            buf_size=buf_size,
            share_path=share_path
        )
        async with maybe_open_fd_share_socket(sh_sock, token.fds):
            yield token

    finally:
        if shm:
            with suppress(Exception):
                shm.unlink()

        if sh_sock and share_path:
            with suppress(Exception):
                sh_sock.close()
                os.unlink(share_path)


'''
IPC MemoryChannel

`eventfd(2)` is used for wrap around sync, to signal writes to
the reader and end of stream.

In order to guarantee full messages are received, all bytes
sent by `MemorySendChannel` are preceded with a 4 byte header
which decodes into a uint32 indicating the actual size of the
next full payload.

'''


PayloadT = TypeVar('PayloadT')


class MemorySendChannel(trio.abc.SendChannel[PayloadT]):
    '''
    Memory channel sender side implementation

    Do not use directly! manage with `attach_to_memory_sender`
    after having opened a mem-chan context with `open_memory_channel`.

    Optional batch mode:

    If `batch_size` > 1 messages wont get sent immediately but will be
    stored until `batch_size` messages are pending, then it will send
    them all at once.

    `batch_size` can be changed dynamically but always call, `flush()`
    right before.

    '''
    def __init__(
        self,
        token: MCToken,
        batch_size: int = 1,
        cleanup: bool = False,
        encoder: Encoder | None = None
    ):
        self._token = MCToken.from_msg(token)
        self.batch_size = batch_size

        # mem-chan os resources
        self._shm: SharedMemory | None = None
        self._write_event = EventFD(self._token.write_eventfd, 'w')
        self._wrap_event = EventFD(self._token.wrap_eventfd, 'r')
        self._eof_event = EventFD(self._token.eof_eventfd, 'w')

        # current write pointer
        self._ptr: int = 0

        # when `batch_size` > 1 store messages on `self._batch` and write them
        # all, once `len(self._batch) == `batch_size`
        self._batch: list[bytes] = []

        # close shm & fds on exit?
        self._cleanup: bool = cleanup

        self._enc: Encoder | None = encoder

        # have we closed this mem-chan?
        # set to `False` on `.open()`
        self._is_closed: bool = True

        # ensure no concurrent `.send_all()` calls
        self._send_all_lock = trio.StrictFIFOLock()

        # ensure no concurrent `.send()` calls
        self._send_lock = trio.StrictFIFOLock()

        # ensure no concurrent `.flush()` calls
        self._flush_lock = trio.StrictFIFOLock()

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
    def ptr(self) -> int:
        return self._ptr

    @property
    def write_fd(self) -> int:
        return self._write_event.fd

    @property
    def wrap_fd(self) -> int:
        return self._wrap_event.fd

    @property
    def pending_msgs(self) -> int:
        return len(self._batch)

    @property
    def must_flush(self) -> bool:
        return self.pending_msgs >= self.batch_size

    async def _wait_wrap(self):
        await self._wrap_event.read()

    async def send_all(self, data: Buffer):
        if self.closed:
            raise trio.ClosedResourceError

        if self._send_all_lock.locked():
            raise trio.BusyResourceError

        async with self._send_all_lock:
            # while data is larger than the remaining buf
            target_ptr = self.ptr + len(data)
            while target_ptr > self.size:
                # write all bytes that fit
                remaining = self.size - self.ptr
                self._shm.buf[self.ptr:] = data[:remaining]
                # signal write and wait for reader wrap around
                self._write_event.write(remaining)
                await self._wait_wrap()

                # wrap around and trim already written bytes
                self._ptr = 0
                data = data[remaining:]
                target_ptr = self._ptr + len(data)

            # remaining data fits on buffer
            self._shm.buf[self.ptr:target_ptr] = data
            self._write_event.write(len(data))
            self._ptr = target_ptr

    async def wait_send_all_might_not_block(self):
        return

    async def flush(
        self,
        new_batch_size: int | None = None
    ) -> None:
        if self.closed:
            raise trio.ClosedResourceError

        async with self._flush_lock:
            for msg in self._batch:
                await self.send_all(msg)

            self._batch = []
            if new_batch_size:
                self.batch_size = new_batch_size

    async def send(self, value: PayloadT) -> None:
        if self.closed:
            raise trio.ClosedResourceError

        if self._send_lock.locked():
            raise trio.BusyResourceError

        raw_value: bytes = (
            value
            if isinstance(value, bytes)
            else
            self._enc.encode(value)
        )

        async with self._send_lock:
            msg: bytes = struct.pack("<I", len(raw_value)) + raw_value
            if self.batch_size == 1:
                if len(self._batch) > 0:
                    await self.flush()

                await self.send_all(msg)
                return

            self._batch.append(msg)
            if self.must_flush:
                await self.flush()

    def open(self):
        try:
            self._shm = SharedMemory(
                name=self._token.shm_name,
                size=self._token.buf_size,
                create=False
            )
            self._write_event.open()
            self._wrap_event.open()
            self._eof_event.open()
            self._is_closed = False

        except Exception as e:
            e.add_note(f'while opening sender for {self._token.to_dict()}')
            raise e

    def _close(self):
        self._eof_event.write(
            self._ptr if self._ptr > 0 else self.size
        )

        if self._cleanup:
            self._write_event.close()
            self._wrap_event.close()
            self._eof_event.close()
            self._shm.close()

        self._is_closed = True

    async def aclose(self):
        if self.closed:
            return

        self._close()

    async def __aenter__(self):
        self.open()
        return self


class MemoryReceiveChannel(trio.abc.ReceiveChannel[PayloadT]):
    '''
    Memory channel receiver side implementation

    Do not use directly! manage with `attach_to_memory_receiver`
    after having opened a mem-chan context with `open_memory_channel`.

    '''
    def __init__(
        self,
        token: MCToken,
        cleanup: bool = False,
        decoder: Decoder | None = None
    ):
        self._token = MCToken.from_msg(token)

        # mem-chan os resources
        self._shm: SharedMemory | None = None
        self._write_event = EventFD(self._token.write_eventfd, 'w')
        self._wrap_event = EventFD(self._token.wrap_eventfd, 'r')
        self._eof_event = EventFD(self._token.eof_eventfd, 'r')

        # current read ptr
        self._ptr: int = 0

        # current write_ptr (max bytes we can read from buf)
        self._write_ptr: int = 0

        # end ptr is used when EOF is signaled, it will contain maximun
        # readable position on buf
        self._end_ptr: int = -1

        # close shm & fds on exit?
        self._cleanup: bool = cleanup

        # have we closed this mem-chan?
        # set to `False` on `.open()`
        self._is_closed: bool = True

        self._dec: Decoder | None = decoder

        # ensure no concurrent `.receive_some()` calls
        self._receive_some_lock = trio.StrictFIFOLock()

        # ensure no concurrent `.receive_exactly()` calls
        self._receive_exactly_lock = trio.StrictFIFOLock()

        # ensure no concurrent `.receive()` calls
        self._receive_lock = trio.StrictFIFOLock()

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
    def ptr(self) -> int:
        return self._ptr

    @property
    def write_fd(self) -> int:
        return self._write_event.fd

    @property
    def wrap_fd(self) -> int:
        return self._wrap_event.fd

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
            self._end_ptr = await self._eof_event.read()

        except EFDReadCancelled:
            ...

        except trio.Cancelled:
            ...

        finally:
            # closing write_event should trigger `EFDReadCancelled`
            # on any pending read
            self._write_event.close()

    def receive_nowait(self, max_bytes: int = _DEFAULT_RB_SIZE) -> bytes:
        '''
        Try to receive any bytes we can without blocking or raise
        `trio.WouldBlock`.

        Returns b'' when no more bytes can be read (EOF signaled & read all).

        '''
        if max_bytes < 1:
            raise ValueError("max_bytes must be >= 1")

        # in case `end_ptr` is set that means eof was signaled.
        # it will be >= `write_ptr`, use it for delta calc
        highest_ptr = max(self._write_ptr, self._end_ptr)

        delta = highest_ptr - self._ptr

        # no more bytes to read
        if delta == 0:
            # if `end_ptr` is set that means we read all bytes before EOF
            if self.eof_was_signaled:
                return b''

            # signal the need to wait on `write_event`
            raise trio.WouldBlock

        # dont overflow caller
        delta = min(delta, max_bytes)

        target_ptr = self._ptr + delta

        # fetch next segment and advance ptr
        segment = bytes(self._shm.buf[self._ptr:target_ptr])
        self._ptr = target_ptr

        if self._ptr == self.size:
            # reached the end, signal wrap around
            self._ptr = 0
            self._write_ptr = 0
            self._wrap_event.write(1)

        return segment

    async def receive_some(self, max_bytes: int = _DEFAULT_RB_SIZE) -> bytes:
        '''
        Receive up to `max_bytes`, if no `max_bytes` is provided
        a reasonable default is used.

        Can return < max_bytes.

        '''
        if self.closed:
            raise trio.ClosedResourceError

        if self._receive_some_lock.locked():
            raise trio.BusyResourceError

        async with self._receive_some_lock:
            try:
                # attempt direct read
                return self.receive_nowait(max_bytes=max_bytes)

            except trio.WouldBlock as e:
                # we have read all we can, see if new data is available
                if not self.eof_was_signaled:
                    # if we havent been signaled about EOF yet
                    try:
                        # wait next write and advance `write_ptr`
                        delta = await self._write_event.read()
                        self._write_ptr += delta
                        # yield lock and re-enter

                    except (
                        EFDReadCancelled,  # read was cancelled with cscope
                        trio.Cancelled,  # read got cancelled from outside
                        trio.BrokenResourceError  # OSError EBADF happened while reading
                    ):
                        # while waiting for new data `self._write_event` was closed
                        try:
                            # if eof was signaled receive no wait will not raise
                            # trio.WouldBlock and will push remaining until EOF
                            return self.receive_nowait(max_bytes=max_bytes)

                        except trio.WouldBlock:
                            # eof was not signaled but `self._wrap_event` is closed
                            # this means send side closed without EOF signal
                            return b''

                else:
                    # shouldnt happen because receive_nowait does not raise
                    # trio.WouldBlock when `end_ptr` is set
                    raise InternalError(
                        'self._end_ptr is set but receive_nowait raised trio.WouldBlock'
                    ) from e

        return await self.receive_some(max_bytes=max_bytes)

    async def receive_exactly(self, num_bytes: int) -> bytes:
        '''
        Fetch bytes until we read exactly `num_bytes` or EOC.

        '''
        if self.closed:
            raise trio.ClosedResourceError

        if self._receive_exactly_lock.locked():
            raise trio.BusyResourceError

        async with self._receive_exactly_lock:
            payload = b''
            while len(payload) < num_bytes:
                remaining = num_bytes - len(payload)

                new_bytes = await self.receive_some(
                    max_bytes=remaining
                )

                if new_bytes == b'':
                    break

                payload += new_bytes

            if payload == b'':
                raise trio.EndOfChannel

            return payload

    async def receive(self, raw: bool = False) -> PayloadT:
        '''
        Receive a complete payload or raise EOC

        '''
        if self.closed:
            raise trio.ClosedResourceError

        if self._receive_lock.locked():
            raise trio.BusyResourceError

        async with self._receive_lock:
            header: bytes = await self.receive_exactly(4)
            size: int
            size, = struct.unpack("<I", header)
            if size == 0:
                raise trio.EndOfChannel

            raw_msg = await self.receive_exactly(size)
            if raw:
                return raw_msg

            return (
                raw_msg
                if not self._dec
                else self._dec.decode(raw_msg)
            )

    async def iter_raw_pairs(self) -> tuple[bytes, PayloadT]:
        if not self._dec:
            raise RuntimeError('iter_raw_pair requires decoder')

        while True:
            try:
                raw = await self.receive(raw=True)
                yield raw, self._dec.decode(raw)

            except trio.EndOfChannel:
                break

    def open(self):
        try:
            self._shm = SharedMemory(
                name=self._token.shm_name,
                size=self._token.buf_size,
                create=False
            )
            self._write_event.open()
            self._wrap_event.open()
            self._eof_event.open()
            self._is_closed = False

        except Exception as e:
            e.add_note(f'while opening receiver for {self._token.to_dict()}')
            raise e

    def close(self):
        if self._cleanup:
            self._write_event.close()
            self._wrap_event.close()
            self._eof_event.close()
            self._shm.close()

        self._is_closed = True

    async def aclose(self):
        if self.closed:
            return

        self.close()

    async def __aenter__(self):
        self.open()
        return self


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

    Launches `receiver._eof_monitor_task` in a `trio.Nursery`.

    '''
    token = MCToken.from_msg(token)

    if token.share_path:
        write, wrap, eof = await recv_fds(token.share_path, 3)
        token = MCToken(
            shm_name=token.shm_name,
            write_eventfd=write,
            wrap_eventfd=wrap,
            eof_eventfd=eof,
            buf_size=token.buf_size,
            share_path=token.share_path
        )
        log.info(f'received fds from {token.share_path}')

    async with (
        trio.open_nursery(strict_exception_groups=False) as n,
        MemoryReceiveChannel(
            token,
            cleanup=cleanup,
            decoder=decoder
        ) as receiver
    ):
        n.start_soon(receiver._eof_monitor_task)
        yield receiver


@acm
async def attach_to_memory_sender(

    token: MCToken | dict,
    *,
    batch_size: int = 1,
    cleanup: bool = False,
    encoder: Encoder | None = None,

) -> AsyncGenerator[MemorySendChannel, None]:
    '''
    Attach a `MemorySendChannel` from a previously opened
    MCToken.

    '''
    token = MCToken.from_msg(token)

    if token.share_path:
        write, wrap, eof = await recv_fds(token.share_path, 3)
        token = MCToken(
            shm_name=token.shm_name,
            write_eventfd=write,
            wrap_eventfd=wrap,
            eof_eventfd=eof,
            buf_size=token.buf_size,
            share_path=token.share_path
        )
        log.info(f'received fds from {token.share_path}')

    async with MemorySendChannel(
        token,
        batch_size=batch_size,
        cleanup=cleanup,
        encoder=encoder
    ) as sender:
        yield sender


class MemoryChannel(trio.abc.Channel[bytes]):
    '''
    Combine `MemorySendChannel` and `MemoryReceiveChannel`
    in order to expose the bidirectional `trio.abc.Channel` API.

    '''
    def __init__(
        self,
        sender: MemorySendChannel,
        receiver: MemoryReceiveChannel
    ):
        self._sender = sender
        self._receiver = receiver

    @property
    def batch_size(self) -> int:
        return self._sender.batch_size

    @batch_size.setter
    def batch_size(self, value: int) -> None:
        self._sender.batch_size = value

    @property
    def pending_msgs(self) -> int:
        return self._sender.pending_msgs

    async def send_all(self, value: bytes) -> None:
        await self._sender.send_all(value)

    async def wait_send_all_might_not_block(self):
        await self._sender.wait_send_all_might_not_block()

    async def flush(
        self,
        new_batch_size: int | None = None
    ) -> None:
        await self._sender.flush(new_batch_size=new_batch_size)

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
    batch_size: int = 1,
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
            batch_size=batch_size,
            cleanup=cleanup_out,
            encoder=encoder,
        ) as sender,
    ):
        yield MemoryChannel(sender, receiver)
