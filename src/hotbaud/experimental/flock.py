'''
Filesystem `flock` based `trio.Lock` like impl

TODO: docstring

'''
import os

from fcntl import flock, LOCK_EX, LOCK_UN, LOCK_NB

from pathlib import Path

import trio


open_mode = os.O_RDWR | os.O_CREAT | os.O_TRUNC


class LockCommon:
    '''
    Common between sync & async Lock impls

    '''
    def __init__(self, path: str | Path):
        self._path = Path(path)
        self._fd = 0
        self._locked = False

    @property
    def fd(self) -> int:
        return self._fd

    @property
    def locked(self) -> bool:
        return self._locked

    def _open(self) -> None:
        self._fd = os.open(self._path, flags=open_mode)

    def _close(self) -> None:
        if self._fd:
            os.close(self._fd)

    def _ensure_locked(self) -> None:
        '''
        Raise if the lock is not held.

        '''
        if not self._locked:
            raise RuntimeError('Tried to release un-acquired lock')

    def release(self) -> None:
        self._ensure_locked()
        flock(self._fd, LOCK_UN)
        self._locked = False


class Lock(LockCommon):
    def acquire(self) -> None:
        if self._locked:
            return

        flock(self._fd, LOCK_EX)
        self._locked = True

    def __enter__(self):
        self._open()
        self.acquire();
        return self

    def __exit__(self, *_):
        self.release()
        self._close()


class AsyncLock(LockCommon):
    '''
    Same as Lock but using trio async
    (requires non blocking flag EFD_NONBLOCK on fd open)

    '''
    async def acquire(self) -> None:
        if self._locked:
            return

        await trio.lowlevel.wait_readable(self._fd)
        flock(self._fd, LOCK_EX | LOCK_NB)
        self._locked = True

    async def __aenter__(self):
        self._open()
        await self.acquire();
        return self

    async def __aexit__(self, *_):
        self.release()
        self._close()
