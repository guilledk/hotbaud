import os
import sys
import threading
import multiprocessing as mp

import pytest
import anyio

from hotbaud import eventfd as efd


LINUX = sys.platform.startswith('linux')
pytestmark = pytest.mark.skipif(
    not LINUX, reason='Linux-only: requires eventfd'
)


def test_open_write_read_close_basic():
    fd = efd.ll_open_eventfd(0, 0)
    try:
        assert fd > 0
        wrote = efd.ll_write_eventfd(fd, 5)
        assert wrote == 8
        val = efd.ll_read_eventfd(fd)
        assert val == 5
    finally:
        efd.ll_close_eventfd(fd)


def test_semaphore_mode_reads_ones():
    fd = efd.ll_open_eventfd(0, efd.EFD_SEMAPHORE)
    try:
        efd.ll_write_eventfd(fd, 3)
        assert efd.ll_read_eventfd(fd) == 1
        assert efd.ll_read_eventfd(fd) == 1
        assert efd.ll_read_eventfd(fd) == 1
    finally:
        efd.ll_close_eventfd(fd)


def test_nonblocking_empty_raises_eagain():
    fd = efd.ll_open_eventfd(0, efd.EFD_NONBLOCK)
    try:
        with pytest.raises(OSError) as ei:
            _ = efd.ll_read_eventfd(fd)
        msg = str(ei.value)
        args0 = ei.value.args[0] if ei.value.args else ''
        assert 'EAGAIN' in msg or args0 == 'EAGAIN'
    finally:
        efd.ll_close_eventfd(fd)


async def test_eventfd_async_read_epoll_backend(anyio_backend):
    async def writer(ev):
        await anyio.sleep(0.05)
        ev.write(2)

    async with efd.open_eventfd(omode='w', sync_backend='epoll') as ev:
        async with anyio.create_task_group() as tg:
            tg.start_soon(writer, ev)
            val = await ev.read()
            assert val == 2


async def test_eventfd_async_read_thread_backend(anyio_backend):
    async def writer(ev):
        await anyio.sleep(0.05)
        ev.write(7)

    async with efd.open_eventfd(omode='w', sync_backend='thread') as ev:
        async with anyio.create_task_group() as tg:
            tg.start_soon(writer, ev)
            val = await ev.read()
            assert val == 7


async def test_read_cancelled_on_close_raises_custom(anyio_backend):
    status = {'ok': False}

    async def reader(ev):
        try:
            await ev.read()
        except efd.EFDReadCancelled:
            status['ok'] = True

    async with efd.open_eventfd(omode='w', sync_backend='epoll') as ev:
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(reader, ev)
                await anyio.sleep(0.05)
                ev.close()
                await anyio.sleep(0.05)
            assert status['ok']
        finally:
            try:
                ev.close()
            except Exception:
                pass


async def test_busy_resource_error_on_concurrent_read(anyio_backend):
    async with efd.open_eventfd(omode='w', sync_backend='epoll') as ev:

        async def first_read():
            await anyio.sleep(0)
            await ev.read()

        async def delayed_write():
            await anyio.sleep(0.1)
            ev.write(1)

        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(first_read)
                tg.start_soon(delayed_write)
                await anyio.sleep(0.01)
                with pytest.raises(anyio.BusyResourceError):
                    await ev.read()
        finally:
            try:
                ev.close()
            except Exception:
                pass


def test_multithreaded_writers_accumulate():
    fd = efd.ll_open_eventfd(0, 0)
    n = 16

    def tfn():
        efd.ll_write_eventfd(fd, 1)

    try:
        threads = [threading.Thread(target=tfn) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        val = efd.ll_read_eventfd(fd)
        assert val == n
    finally:
        efd.ll_close_eventfd(fd)


async def test_subprocess_writer_delivers_value_via_anyio(anyio_backend):
    fd = efd.ll_open_eventfd(0, 0)
    try:
        os.set_inheritable(fd, True)

        code = (
            'import os, importlib\n'
            'fd = int(os.environ["EFD_FD"])\n'
            'value = int(os.environ["EFD_VALUE"])\n'
            'from hotbaud import eventfd as efd\n'
            'efd.ll_write_eventfd(fd, value)\n'
            'efd.ll_close_eventfd(fd)\n'
        )
        env = os.environ.copy()
        env.update({'EFD_FD': str(fd), 'EFD_VALUE': '42'})

        proc = await anyio.open_process(
            [sys.executable, '-c', code], env=env, pass_fds=[fd]
        )
        rc = await proc.wait()

        assert rc == 0
        val = efd.ll_read_eventfd(fd)
        assert val == 42
    finally:
        efd.ll_close_eventfd(fd)
