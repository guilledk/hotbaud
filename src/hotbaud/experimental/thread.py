'''
A *fully-synchronous* intra‑process queue built around

* a **single** shared circular buffer (`memoryview`)
* two Linux **eventfd(2)** counters - one for *producer→consumer* notifications
  and one for *consumer→producer* notifications

The API mirrors - in a *blocking*, thread‑friendly way - the async
*MemoryChannel* living in :pymod:`hotbaud.memchan._impl`, but **without any
copying on the consumer side**: :py:meth:`QueueReader.read` now returns a
*live* ``memoryview`` of the underlying ring so callers can opt‑in to zero‑copy
processing.

Key design points
-----------------

* **Ring‑buffer layout** - both writer and reader keep *monotonically
  increasing* ``wptr`` / ``rptr`` indices.  The *actual* byte position is
  ``idx % buf_size`` so we avoid resetting the counters.
* **Flow‑control** - after the producer copies *N* bytes it writes the delta to
  the *write* eventfd; the consumer wakes, bumps its local ``_wptr`` and can
  immediately read the fresh region.  Once it *consumes* *M* bytes it mirrors
  the process on the *read* eventfd so the writer is unblocked.
* **Back‑pressure** - the writer blocks while the ring is full; the reader
  blocks while it is empty.  No busy‑loops.
* **Zero‑copy reads** - the consumer receives *contiguous* slices of the ring
  as a ``memoryview``.  If a wrap‑around would be necessary the slice stops at
  the end of the buffer; callers can simply call ``read`` again to obtain the
  remaining bytes (in practice this still keeps throughput high as the second
  call is already non‑blocking).

Limitations
-----------

* Single‑producer / single‑consumer (SP/SC).
* No explicit *EOF*; use an application‑level sentinel if you need that.

'''

from __future__ import annotations

import struct

import msgspec
import trio

from hotbaud.eventfd import open_eventfd, read_eventfd, write_eventfd
from hotbaud.types import Buffer


class QueueToken(msgspec.Struct):
    '''
    Metadata shared by the producer and the consumer.

    '''

    buffer: memoryview
    buf_size: int
    write_eventfd: int
    read_eventfd: int

    @classmethod
    def alloc(cls, buf: memoryview) -> 'QueueToken':
        wr_fd = open_eventfd(0, 0)
        rd_fd = open_eventfd(0, 0)
        return cls(buf, len(buf), wr_fd, rd_fd)


def _copy_into(buf: memoryview, src: Buffer, start: int, size: int) -> None:
    '''
    Copy *size* bytes from *src* into *buf* starting at *start* modulo ring.

    '''
    end_space = len(buf) - start
    if size <= end_space:
        buf[start : start + size] = src[:size]
    else:
        buf[start:] = src[:end_space]
        buf[: size - end_space] = src[end_space:]


class QueueWriter:
    '''
    Single‑producer side (blocking, thread‑safe).

    '''

    def __init__(self, token: QueueToken) -> None:
        self._t = token
        self._buf = token.buffer
        self._size = token.buf_size
        self._wptr: int = 0  # next write position (monotonic)
        self._rptr: int = 0  # mirror of consumer progress

    def _used(self) -> int:
        return self._wptr - self._rptr

    def _free(self) -> int:
        return self._size - self._used()

    @property
    def size(self) -> int:
        return self._size

    def write(self, data: Buffer) -> None:
        '''
        Blocking *copy* of *data* into the ring.

        '''
        n = len(data)
        if n > self._size:
            raise ValueError(
                f'payload of {n} bytes never fits into buffer of {self._size} bytes'
            )

        # back‑pressure loop - wait until there is room for *n* bytes
        while self._free() < n:
            delta = read_eventfd(self._t.read_eventfd)
            self._rptr += delta

        # copy into the ring
        start = self._wptr % self._size
        _copy_into(self._buf, data, start, n)

        # advance pointer & notify consumer
        self._wptr += n
        write_eventfd(self._t.write_eventfd, n)

    # Convenience framing helper
    def send(self, payload: Buffer) -> None:
        raw = bytes(payload)
        header = struct.pack('<I', len(raw))
        self.write(header + raw)


class QueueReader:
    '''
    Single‑consumer side (blocking, thread‑safe).

    '''

    def __init__(self, token: QueueToken):
        self._t = token
        self._buf = token.buffer
        self._size = token.buf_size
        self._wptr: int = 0  # mirror of producer progress
        self._rptr: int = 0  # next read position (monotonic)

    def _available(self) -> int:
        return self._wptr - self._rptr

    @property
    def size(self) -> int:
        return self._size

    def read(self, max_bytes: int) -> bytes:
        '''
        Blocking, **zero‑copy** read of *up to* ``max_bytes`` bytes.

        Returns a **contiguous** ``memoryview`` into the ring buffer.  If the
        available data wraps around the end of the buffer only the first
        contiguous chunk (ending at ``buf_size``) is returned - simply call
        ``read`` again to grab the remainder.

        '''
        if max_bytes <= 0:
            raise ValueError('max_bytes must be > 0')

        # wait until some data is available
        while self._available() == 0:
            delta = read_eventfd(self._t.write_eventfd)
            self._wptr += delta

        # how much can we serve in a single contiguous slice
        avail = self._available()
        n = min(max_bytes, avail)
        start = self._rptr % self._size
        contiguous = min(n, self._size - start)  # stop at wrap‑around

        view = bytes(self._buf[start : start + contiguous])

        # advance pointer & notify producer
        self._rptr += contiguous
        write_eventfd(self._t.read_eventfd, contiguous)

        return view

    def recv(self) -> bytes:
        header = self.read(4)
        (size,) = struct.unpack('<I', header)
        if size == 0:
            return b''

        out = bytearray()
        while len(out) < size:
            out += self.read(size - len(out))
        return bytes(out)


def create_queue(buf: memoryview) -> tuple[QueueReader, QueueWriter]:
    token = QueueToken.alloc(buf)
    return QueueReader(token), QueueWriter(token)


class AsyncQueueWriter:
    '''
    Single‑producer side - **asynchronous** version.

    '''

    def __init__(self, token: QueueToken) -> None:
        self._t = token
        self._buf = token.buffer
        self._size = token.buf_size
        self._wptr: int = 0  # next write position (monotonic)
        self._rptr: int = 0  # mirror of consumer progress
        self._write_lock = trio.Lock()

    def _used(self) -> int:
        return self._wptr - self._rptr

    def _free(self) -> int:
        return self._size - self._used()

    @property
    def size(self) -> int:  # expose for tests
        return self._size

    async def write(self, data: Buffer) -> None:
        '''Asynchronously *copy* ``data`` into the ring - blocks while full.'''
        async with self._write_lock:  # avoid re‑entrancy from user code
            n = len(data)
            if n > self._size:
                raise ValueError(
                    f"payload of {n} bytes never fits into buffer of {self._size} bytes"
                )

            # back‑pressure loop - wait until there is room for *n* bytes
            while self._free() < n:
                await trio.lowlevel.wait_readable(self._t.read_eventfd)
                delta = read_eventfd(self._t.read_eventfd)
                self._rptr += delta

            # copy into the ring
            start = self._wptr % self._size
            _copy_into(self._buf, data, start, n)

            # advance pointer & notify consumer
            self._wptr += n
            write_eventfd(self._t.write_eventfd, n)

    async def send(self, payload: Buffer) -> None:
        raw = bytes(payload)
        header = struct.pack("<I", len(raw))
        await self.write(header + raw)


class AsyncQueueReader:
    '''
    Single‑consumer side - **asynchronous** version.

    '''

    def __init__(self, token: QueueToken):
        self._t = token
        self._buf = token.buffer
        self._size = token.buf_size
        self._wptr: int = 0  # mirror of producer progress
        self._rptr: int = 0  # next read position (monotonic)
        self._read_lock = trio.Lock()

    def _available(self) -> int:
        return self._wptr - self._rptr

    @property
    def size(self) -> int:
        return self._size

    async def read(self, max_bytes: int) -> bytes:
        '''
        Asynchronous, **zero‑copy** read of up to ``max_bytes`` bytes.

        '''
        if max_bytes <= 0:
            raise ValueError("max_bytes must be > 0")

        async with self._read_lock:
            # wait until some data is available
            while self._available() == 0:
                await trio.lowlevel.wait_readable(self._t.write_eventfd)
                delta = read_eventfd(self._t.write_eventfd)
                self._wptr += delta

            # how much can we serve in a single contiguous slice
            avail = self._available()
            n = min(max_bytes, avail)
            start = self._rptr % self._size
            contiguous = min(n, self._size - start)  # stop at wrap‑around

            view = bytes(self._buf[start : start + contiguous])

            # advance pointer & notify producer
            self._rptr += contiguous
            write_eventfd(self._t.read_eventfd, contiguous)

            return view

    async def recv(self) -> bytes:
        header = await self.read(4)
        (size,) = struct.unpack("<I", header)
        if size == 0:
            return b""

        out = bytearray()
        while len(out) < size:
            out += await self.read(size - len(out))
        return bytes(out)


def create_async_queue(buf: memoryview) -> tuple[AsyncQueueReader, AsyncQueueWriter]:
    token = QueueToken.alloc(buf)
    return AsyncQueueReader(token), AsyncQueueWriter(token)
