'''
IPC/thread/task Lock

Requires EFD_SEMAPHORE on eventfd open

TODO: docstring

TODO: considerations of using EFD_NONBLOCK on open: will force sync readers to do polling

'''

import os

from typing import AsyncGenerator, Generator, Type
from contextlib import asynccontextmanager, contextmanager

from anyio.abc import AsyncBackend
from anyio._core._eventloop import get_async_backend
from hotbaud.eventfd import (
    EFD_NONBLOCK,
    EFD_SEMAPHORE,
    ll_open_eventfd,
    ll_read_eventfd,
    ll_write_eventfd,
)


class Lock:
    '''
    Common between sync & async Lock impls

    '''

    def __init__(self, fd: int):
        self._fd = fd
        self._locked = False

    @property
    def fd(self) -> int:
        return self._fd

    @property
    def locked(self) -> bool:
        return self._locked

    def _ensure_locked(self) -> None:
        '''
        Raise if the lock is not held.

        '''
        if not self._locked:
            raise RuntimeError('Tried to release un-acquired lock')

    def release(self) -> None:
        self._ensure_locked()
        self._locked = False
        ll_write_eventfd(self._fd, 1)

    def acquire(self) -> None:
        if self._locked:
            return

        ll_read_eventfd(self._fd)
        self._locked = True

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *_):
        self.release()


class AsyncLock(Lock):
    '''
    Same as Lock but using trio async
    (requires non blocking flag EFD_NONBLOCK on fd open)

    '''

    def __init__(self, fd: int, async_backend: type[AsyncBackend]):
        super().__init__(fd)
        self._async_backend = async_backend

    async def acquire_async(self) -> None:
        if self._locked:
            return

        await self._async_backend.wait_readable(self._fd)
        ll_read_eventfd(self._fd)
        self._locked = True

    async def __aenter__(self):
        await self.acquire_async()
        return self

    async def __aexit__(self, *_):
        self.release()


@contextmanager
def open_lock(
    flags: int = EFD_SEMAPHORE,
) -> Generator[Lock, None, None]:
    '''
    Allocate and manage resources for an LockCommon subclass

    flags: override default eventfd flags

    '''
    fd = ll_open_eventfd(flags=flags)
    fobj = os.fdopen(fd, 'rw')

    yield Lock(fd)

    if fobj:
        fobj.close()


@asynccontextmanager
async def open_async_lock(
    flags: int = EFD_NONBLOCK | EFD_SEMAPHORE,
) -> AsyncGenerator[AsyncLock, None]:
    '''
    Allocate and manage resources for an LockCommon subclass

    flags: override default eventfd flags

    '''
    fd = ll_open_eventfd(flags=flags)
    fobj = os.fdopen(fd, 'rw')

    yield AsyncLock(fd, async_backend=get_async_backend())

    if fobj:
        fobj.close()


@contextmanager
def attach_lock(
    fd: int,
) -> Generator[Lock, None, None]:
    '''
    Attach to an already opened Lock

    fd: underlying eventfd file descriptor

    '''
    with os.fdopen(fd, mode='rw'):
        yield Lock(fd)


@asynccontextmanager
async def attach_async_lock(
    fd: int,
) -> AsyncGenerator[AsyncLock, None]:
    '''
    Attach to an already opened Lock

    fd: underlying eventfd file descriptor

    '''
    with os.fdopen(fd, mode='rw'):
        yield AsyncLock(fd, async_backend=get_async_backend())
