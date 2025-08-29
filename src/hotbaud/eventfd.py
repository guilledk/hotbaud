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
Expose libc eventfd APIs

'''

from contextlib import asynccontextmanager
import os
import errno
from typing import AsyncGenerator, Literal

from anyio.abc import AsyncBackend
import cffi
import anyio

from anyio._core._eventloop import get_async_backend

ffi = cffi.FFI()

# Declare the C functions and types we plan to use.
#    - eventfd: for creating the event file descriptor
#    - write:   for writing to the file descriptor
#    - read:    for reading from the file descriptor
#    - close:   for closing the file descriptor
ffi.cdef(
    '''
    int eventfd(unsigned int initval, int flags);

    ssize_t write(int fd, const void *buf, size_t count);
    ssize_t read(int fd, void *buf, size_t count);

    int close(int fd);
    '''
)


# Open the default dynamic library (essentially 'libc' in most cases)
C = ffi.dlopen(None)


# Constants from <sys/eventfd.h>, if needed.
EFD_SEMAPHORE = 1
EFD_CLOEXEC = 0o2000000
EFD_NONBLOCK = 0o4000


def ll_open_eventfd(initval: int = 0, flags: int = 0) -> int:
    '''
    Open an eventfd with the given initial value and flags.
    Returns the file descriptor on success, otherwise raises OSError.

    '''
    fd = C.eventfd(initval, flags)
    if fd < 0:
        raise OSError(errno.errorcode[ffi.errno], 'eventfd failed')
    return fd


def ll_write_eventfd(fd: int, value: int) -> int:
    '''
    Write a 64-bit integer (uint64_t) to the eventfd's counter.

    '''
    # Create a uint64_t* in C, store `value`
    data_ptr = ffi.new('uint64_t *', value)

    # Call write(fd, data_ptr, 8)
    # We expect to write exactly 8 bytes (sizeof(uint64_t))
    ret = C.write(fd, data_ptr, 8)
    if ret < 0:
        raise OSError(errno.errorcode[ffi.errno], 'write to eventfd failed')
    return ret


def ll_read_eventfd(fd: int) -> int:
    '''
    Read a 64-bit integer (uint64_t) from the eventfd, returning the value.
    Reading resets the counter to 0 (unless using EFD_SEMAPHORE).

    '''
    # Allocate an 8-byte buffer in C for reading
    buf = ffi.new('char[]', 8)

    ret = C.read(fd, buf, 8)
    if ret < 0:
        raise OSError(errno.errorcode[ffi.errno], 'read from eventfd failed')
    # Convert the 8 bytes we read into a Python integer
    data_bytes = ffi.unpack(buf, 8)  # returns a Python bytes object of length 8
    value = int.from_bytes(data_bytes, byteorder='little', signed=False)
    return value


def ll_close_eventfd(fd: int) -> int:
    '''
    Close the eventfd.

    '''
    ret = C.close(fd)
    if ret < 0:
        raise OSError(errno.errorcode[ffi.errno], 'close failed')


EFDSyncMethods = Literal['epoll', 'thread']
EFDOpenMode = Literal['r', 'w', 'rw']

default_sync_method: EFDSyncMethods = 'epoll'


class EFDReadCancelled(Exception): ...


class EventFD:
    '''
    Use a previously opened eventfd(2), meant to be used in
    sub-processes after root process opens the eventfds then passes
    them through pass_fds

    '''

    def __init__(
        self,
        fd: int,
        omode: EFDOpenMode,
        sync_backend: EFDSyncMethods,
        async_backend: type[AsyncBackend],
    ):
        self._fd: int = fd
        self._omode: str = omode
        self._cscope: anyio.CancelScope | None = None
        self._is_closed: bool = True
        self._read_lock = anyio.Lock()

        self._async_backend = async_backend

        self.sync_backend = sync_backend

    @property
    def closed(self) -> bool:
        return self._is_closed

    @property
    def fd(self) -> int:
        return self._fd

    def write(self, value: int) -> int:
        if self.closed:
            raise anyio.ClosedResourceError

        return ll_write_eventfd(self._fd, value)

    async def read(self) -> int:
        '''
        Async wrapper for `read_eventfd(self.fd)`

        '''
        if self.closed:
            raise anyio.ClosedResourceError

        if self._read_lock.locked():
            raise anyio.BusyResourceError(action='reading')

        async with self._read_lock:
            value: int | None = None
            self._cscope = anyio.CancelScope()
            with self._cscope:
                try:
                    match self.sync_backend:
                        case 'epoll':
                            await self._async_backend.wait_readable(self._fd)
                            value = self.read_nowait()

                        case 'thread':
                            return await anyio.to_thread.run_sync(
                                ll_read_eventfd,
                                self._fd,
                                abandon_on_cancel=True,
                            )

                except OSError as e:
                    if e.errno == 'EAGAIN':
                        raise anyio.WouldBlock

                    raise anyio.BrokenResourceError from e

            if value is None or self._cscope.cancelled_caught:
                raise EFDReadCancelled

            self._cscope = None

            return value

    def read_nowait(self) -> int:
        '''
        Direct call to `read_eventfd(self.fd)`, unless `eventfd` was
        opened with `EFD_NONBLOCK` its gonna block the thread.

        '''
        return ll_read_eventfd(self._fd)

    def open(self):
        self._is_closed = False

    def close(self):
        if self._cscope:
            self._cscope.cancel()

        self._is_closed = True

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


@asynccontextmanager
async def open_eventfd(
    fd: int | None = None,
    omode: EFDOpenMode = 'rw',
    sync_backend: EFDSyncMethods = 'epoll',
    initval: int = 0,
    flags: int = 0,
) -> AsyncGenerator[EventFD, None]:
    if fd is None:
        fd = ll_open_eventfd(initval, flags)

    with EventFD(fd, omode, sync_backend, get_async_backend()) as e:
        yield e
