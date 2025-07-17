'''
IPC/thread/task Lock

Requires EFD_SEMAPHORE on eventfd open

TODO: docstring

TODO: considerations of using EFD_NONBLOCK on open: will force sync readers to do polling

'''
import os

from typing import Generator, Type
from contextlib import contextmanager

import trio
from hotbaud.eventfd import EFD_NONBLOCK, EFD_SEMAPHORE, open_eventfd, read_eventfd, write_eventfd


class LockCommon:
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
        write_eventfd(self._fd, 1)


class Lock(LockCommon):
    def acquire(self) -> None:
        if self._locked:
            return

        read_eventfd(self._fd)
        self._locked = True

    def __enter__(self):
        self.acquire();
        return self

    def __exit__(self, *_):
        self.release()


class AsyncLock(LockCommon):
    '''
    Same as Lock but using trio async
    (requires non blocking flag EFD_NONBLOCK on fd open)

    '''
    async def acquire(self) -> None:
        if self._locked:
            return

        await trio.lowlevel.wait_readable(self._fd)
        read_eventfd(self._fd)
        self._locked = True

    async def __aenter__(self):
        await self.acquire();
        return self

    async def __aexit__(self, *_):
        self.release()


@contextmanager
def open_lock(
    flags: int | None = None,
    lock_type: Type[LockCommon] = Lock
) -> Generator[LockCommon, None, None]:
    '''
    Allocate and manage resources for an LockCommon subclass

    flags: override default eventfd flags
    lock_type: choose LockCommon subclass to yield

    '''
    flags = (
        (EFD_NONBLOCK if lock_type == AsyncLock else 0) | EFD_SEMAPHORE
        if flags is None
        else flags
    )

    fd = open_eventfd(flags=flags)
    fobj = os.fdopen(fd, 'rw')

    yield lock_type(fd)

    if fobj:
        fobj.close()


@contextmanager
def attach_lock(
    fd: int,
    lock_type: Type[LockCommon] = Lock
) -> Generator[LockCommon, None, None]:
    '''
    Attach to an already opened Lock

    fd: underlying eventfd file descriptor
    lock_type: choose LockCommon subclass to yield

    '''
    with os.fdopen(fd, mode='rw'):
        yield lock_type(fd)
