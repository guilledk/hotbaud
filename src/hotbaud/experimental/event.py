'''
IPC/thread/task Event

Overview:

- One to many: wake up many workers with one set.
- One-off: non re-entrant, attempting to reuse the Event will:
    - Trying to re-set it will either raise (on same instance) or when done
      from a separate Event instance will block the thread (or raise EAGAIN
      OSError if using EFD_NONBLOCK) if done using a different Event instance.

    - Trying to wait on an already set event will raise (on same instance) or
      return immediately, unless this Event has already woken up more than
      the largest unsigned 64-bit value minus 1 waiters, in which case it will:

          - When using threads: block (or raise EAGAIN OSError if using
            EFD_NONBLOCK).

          - When using trio tasks: raise trio.WouldBlock

To ensure this properties its assumed the underlying eventfd(2) is opened with
EFD_SEMAPHORE flag, which will make readers substract one from the os counter
instead of resetting it to 0, and it the setter writes uint64 max - 1, which
will make any subsequent setter block or raise an error guaranteeing non
re-entrancy.

Exerpt from eventfd manual:

`man eventfd`

> The maximum value that may be stored in the counter is the largest unsigned
64-bit value minus 1 (i.e., 0xfffffffffffffffe).  If the addition would cause
the counter's value to exceed the maximum, then the write(2) either blocks
until a read(2) is performed on the file descriptor, or fails with the error
EAGAIN if the file descriptor has been made nonblocking.

'''
import os

from typing import Generator, Type
from contextlib import contextmanager

import trio
from hotbaud.eventfd import EFD_NONBLOCK, EFD_SEMAPHORE, open_eventfd, read_eventfd, write_eventfd


default_set_value = (2 ** 64) - 1


class EventCommon:
    '''
    Common between sync & async Event impls

    '''
    def __init__(self, fd: int, value: int = default_set_value):
        self._fd = fd
        self._is_set = False
        self.value = value

    @property
    def fd(self) -> int:
        return self._fd

    @property
    def is_set(self) -> bool:
        return self._is_set

    def _ensure_unset(self) -> None:
        '''
        Raise if this Event instance has been used already

        '''
        if self._is_set:
            raise RuntimeError('Event already set')

    def set(self) -> None:
        '''
        Signal the event happened, wake up any current or future waiters

        '''
        self._ensure_unset()
        write_eventfd(self._fd, self.value)
        self._is_set = True


class Event(EventCommon):
    def wait(self) -> None:
        self._ensure_unset()
        read_eventfd(self._fd)
        self._is_set = True


class AsyncEvent(EventCommon):
    '''
    Same as Event but using trio async
    (requires non blocking flag EFD_NONBLOCK on fd open)

    '''
    async def wait(self) -> None:
        self._ensure_unset()
        await trio.lowlevel.wait_readable(self._fd)
        try:
            read_eventfd(self._fd)

        except OSError as e:
            if e.errno == 'EAGAIN':
                raise trio.WouldBlock

            else:
                raise

        self._is_set = True


@contextmanager
def open_event(
    value: int = default_set_value,
    mode: str = 'rw',
    flags: int | None = None,
    event_type: Type[EventCommon] = Event
) -> Generator[EventCommon, None, None]:
    '''
    Allocate and manage resources for an EventCommon subclass

    value: configure the value added to counter when calling .set
    flags: override default eventfd flags
    mode: limit to reading or writing, or allow both (default)
    event_type: choose EventCommon subclass to yield

    '''
    flags = (
        (EFD_NONBLOCK if event_type == AsyncEvent else 0) | EFD_SEMAPHORE
        if flags is None
        else flags
    )

    fd = open_eventfd(flags=flags)
    fobj = os.fdopen(fd, mode)

    yield event_type(fd, value=value)

    if fobj:
        fobj.close()


@contextmanager
def attach_event(
    fd: int,
    value: int = default_set_value,
    mode: str = 'rw',
    event_type: Type[EventCommon] = Event
) -> Generator[EventCommon, None, None]:
    '''
    Attach to an already opened Event

    fd: underlying eventfd file descriptor
    value: configure the value added to counter when calling .set
    mode: limit to reading or writing, or allow both (default)
    event_type: choose EventCommon subclass to yield

    '''
    with os.fdopen(fd, mode=mode):
        yield event_type(fd, value=value)
