import time
from hotbaud.experimental.pipeline import Port


async def byte_fiend(
    worker_id: str, in_port: Port, slot_size: int, msg_size: int, **kwargs
) -> None:
    slot_bytes = 0
    slot_mono = time.perf_counter()
    async with in_port.attach() as chan:
        async for msg in chan:
            slot_bytes += len(msg)
            if slot_bytes >= slot_size:
                now = time.perf_counter()
                elapsed = now - slot_mono
                bytes_per_sec = slot_bytes / elapsed
                msg_per_sec = bytes_per_sec / msg_size
                print(
                    f'{bytes_per_sec:,.1f} bytes / s, {msg_per_sec:,.1f} msgs / s'
                )
                slot_mono = now
                slot_bytes = 0


async def byte_pusher(
    worker_id: str, out_port: Port, amount_gb: int, msg_size: int, **kwargs
) -> None:
    packet = b'1' * msg_size
    msg_amount = int((amount_gb * 1024 * 1024 * 1024) // msg_size)
    print(msg_amount)
    async with out_port.attach() as chan:
        for _ in range(msg_amount):
            await chan.send(packet)
