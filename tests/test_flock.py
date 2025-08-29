import os
import time
import threading

import pytest

from hotbaud.experimental.flock import Lock, AsyncLock


def tmp_lock_path(tmp_path_factory):
    '''Return a unique path for each test without touching /tmp directly.'''
    return tmp_path_factory.mktemp('locks') / 'lockfile'


def test_lock_basic(tmp_path_factory):
    path = tmp_lock_path(tmp_path_factory)

    # context-manager acquires and releases
    with Lock(path) as l:
        assert l.locked is True
        assert os.path.isfile(path)

    assert l.locked is False  # released on __exit__


async def test_async_lock_basic(anyio_backend, tmp_path_factory):
    path = tmp_lock_path(tmp_path_factory)

    async with AsyncLock(path) as l:
        assert l.locked is True
    assert l.locked is False


def test_lock_reentry_and_double_release(tmp_path_factory):
    path = tmp_lock_path(tmp_path_factory)
    lock = Lock(path)
    lock._open()

    lock.acquire()
    lock.acquire()  # re-acquire, must not raise
    lock.release()

    with pytest.raises(RuntimeError):
        lock.release()  # cannot release twice

    lock._close()


async def test_async_lock_reentry_and_double_release(
    anyio_backend, tmp_path_factory
):
    path = tmp_lock_path(tmp_path_factory)
    lock = AsyncLock(path)
    lock._open()

    await lock.acquire_async()
    await lock.acquire_async()
    lock.release()

    with pytest.raises(RuntimeError):
        lock.release()

    lock._close()


def test_lock_blocks_until_released(tmp_path_factory):
    path = tmp_lock_path(tmp_path_factory)
    delay = 0.2
    waited = []
    ready = threading.Event()

    def holder():
        with Lock(path):
            ready.set()  # signal we have the lock
            time.sleep(delay)

    def contender():
        ready.wait()  # wait until holder really owns it
        start = time.perf_counter()
        with Lock(path):
            waited.append(time.perf_counter() - start)

    t1 = threading.Thread(target=holder, daemon=True)
    t2 = threading.Thread(target=contender, daemon=True)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert waited and waited[0] >= delay * 0.9
