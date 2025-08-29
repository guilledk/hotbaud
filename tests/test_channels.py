import sys
from uuid import uuid4

import anyio
import pytest
from anyio import to_process

from hotbaud import MCToken
from hotbaud.memchan._impl import (
    attach_to_memory_receiver,
    attach_to_memory_sender,
    open_memory_channel,
)


def sender_process(
    token: MCToken,
    msgs: list[str],
) -> None:
    async def _main():
        async with attach_to_memory_sender(token) as sender:
            for msg in msgs:
                payload = msg if isinstance(msg, bytes) else msg.encode()
                await sender.send(payload)

    anyio.run(_main, backend='trio')


def receiver_process(token: MCToken) -> list[str]:
    async def _main() -> list[str]:
        async with attach_to_memory_receiver(token) as receiver:
            return [msg.decode() async for msg in receiver]

    return anyio.run(_main, backend='trio')


pytestmark = [
    pytest.mark.anyio,
    pytest.mark.skipif(
        sys.platform != 'linux',
        reason='Linux-only: relies on eventfd(2)',
    ),
]


def _shm_name() -> str:
    return f'hb-test-{uuid4().hex}'


def _share_path() -> str:
    return f'/tmp/hb-fdshare-{uuid4().hex}.sock'


@pytest.mark.parametrize('sync_backend', ['epoll', 'thread'])
async def test_ipc_channel_msg_arrival(anyio_backend, sync_backend):
    shm_name = _shm_name()
    share_path = _share_path()
    msgs = ['one', 'two', 'three', 'four']

    with anyio.fail_after(5):
        async with open_memory_channel(
            shm_name,
            buf_size=256,
            share_path=share_path,
            sync_backend=sync_backend,
        ) as token:
            async with anyio.create_task_group() as tg:
                tg.start_soon(to_process.run_sync, sender_process, token, msgs)
                received = await to_process.run_sync(receiver_process, token)

            assert received == msgs


@pytest.mark.parametrize('sync_backend', ['epoll', 'thread'])
async def test_ipc_channel_wraparound_and_ordering(anyio_backend, sync_backend):
    shm_name = _shm_name()
    share_path = _share_path()
    msgs = [f'msg-{i:03d}-' + ('x' * 8) for i in range(60)]

    with anyio.fail_after(10):
        async with open_memory_channel(
            shm_name,
            buf_size=64,
            share_path=share_path,
            sync_backend=sync_backend,
        ) as token:
            async with anyio.create_task_group() as tg:
                tg.start_soon(to_process.run_sync, sender_process, token, msgs)
                received = await to_process.run_sync(receiver_process, token)

            assert received == msgs


@pytest.mark.parametrize('sync_backend', ['epoll', 'thread'])
async def test_ipc_channel_zero_messages(anyio_backend, sync_backend):
    shm_name = _shm_name()
    share_path = _share_path()

    with anyio.fail_after(5):
        async with open_memory_channel(
            shm_name,
            buf_size=64,
            share_path=share_path,
            sync_backend=sync_backend,
        ) as token:
            async with anyio.create_task_group() as tg:
                tg.start_soon(to_process.run_sync, sender_process, token, [])
                received = await to_process.run_sync(receiver_process, token)

            assert received == []


@pytest.mark.parametrize('sync_backend', ['epoll', 'thread'])
async def test_receive_exactly_raises_on_eof(anyio_backend, sync_backend):
    shm_name = _shm_name()

    with anyio.fail_after(5):
        async with open_memory_channel(
            shm_name,
            buf_size=64,
            sync_backend=sync_backend,
        ) as token:
            async with attach_to_memory_receiver(token) as receiver:
                async with attach_to_memory_sender(token) as sender:
                    await sender.send(b'abc')
                with pytest.raises(anyio.EndOfStream):
                    await receiver.receive_exactly(1000)


@pytest.mark.parametrize('sync_backend', ['epoll', 'thread'])
async def test_send_all_is_single_writer(anyio_backend, sync_backend):
    shm_name = _shm_name()

    with anyio.fail_after(10):
        async with open_memory_channel(
            shm_name,
            buf_size=64,
            sync_backend=sync_backend,
        ) as token:
            async with attach_to_memory_receiver(token) as receiver:
                async with attach_to_memory_sender(token) as sender:
                    big = b'x' * (64 * 4)

                    async def drain(total: int):
                        got = 0
                        while got < total:
                            chunk = await receiver.receive_some(32)
                            if chunk == b'':
                                break
                            got += len(chunk)

                    async with anyio.create_task_group() as tg:
                        tg.start_soon(sender.send_all, big)
                        await anyio.sleep(0.05)

                        async def second():
                            with pytest.raises(anyio.BusyResourceError):
                                await sender.send_all(b'y')

                        tg.start_soon(second)
                        tg.start_soon(drain, len(big))
