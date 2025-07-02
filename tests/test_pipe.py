from functools import partial

from hotbaud.eventfd import EFDSyncMethods
import pytest

from hotbaud.experimental.pipeline import PipelineBuilder
from hotbaud.testing import byte_fiend, byte_pusher


size_small = 4 * 1024  # 4kb
size_medium = 8 * 1024 * 1024  # 8mb
size_large = 256 * 1024 * 1024  # 256mb

msg_small = 256
msg_medium = 8 * 1024  # 8kb
msg_large = 1 * 1024 * 1024  # 1mb

# measure time every `slot_size` amount of bytes received
slot_small = 1 * 1024 * 1024
slot_medium = 16 * 1024 * 1024
slot_large = 1024 * 1024 * 1024


@pytest.mark.parametrize(
    ('amount_gb', 'buf_size', 'msg_size', 'sync_backend', 'slot_size'),
    (
        (0.1, size_small,  msg_small,  'epoll', slot_small),
        (1,   size_medium, msg_medium, 'epoll', slot_medium),
        (10,  size_large,  msg_large,  'epoll', slot_large),
        # (1,  size_small,  msg_small,  'thread', slot_small),
        # (5,  size_medium, msg_medium, 'thread', slot_medium),
        (10,  size_large,  msg_large,  'thread', slot_large),
    )
)
async def test_throughput(
    tmp_path_factory,
    amount_gb: int,
    buf_size: int,
    msg_size: int,
    sync_backend: EFDSyncMethods,
    slot_size: int
):
    builder = (
        PipelineBuilder(
            'pusher-fiend-pattern',
            sync_backend=sync_backend,
            share_path=tmp_path_factory.mktemp('.hotbaud')
        )
        .stage(
            'pusher',
            partial(
                byte_pusher,
                amount_gb=amount_gb,
                msg_size=msg_size
            ),
            outputs='bytes',
            out_size=buf_size
        )
        .stage(
            'fiend',
            partial(
                byte_fiend,
                slot_size=slot_size,
                msg_size=msg_size
            ),
            inputs='bytes',
            in_size=buf_size,
        )
    )

    pipe = await builder.build()

    async with pipe:
        await pipe.run()
