import os, random, struct, threading, time
import trio, pytest

from hotbaud.experimental.thread import (
    QueueToken,
    create_queue,
    create_async_queue,
    QueueReader,
    QueueWriter,
    AsyncQueueReader,
    AsyncQueueWriter,
)

BUF_SIZE = 64 * 1024
MSG_SIZES = [1, 7, 511, 4096]
ROUNDS = 4_000
SEED = 42


def producer_sync(writer: QueueWriter, msgs, delay=0):
    for p in msgs:
        if delay:
            time.sleep(delay)
        writer.send(p)
    writer.send(b'')


def consumer_sync(reader: QueueReader, sink, delay=0):
    while True:
        if delay:
            time.sleep(delay)
        m = reader.recv()
        if not m:
            break
        sink.append(m)


async def producer_async(writer: AsyncQueueWriter, msgs):
    for p in msgs:
        await writer.send(p)
        await trio.sleep(random.random() * 0.002)
    await writer.send(b'')


async def consumer_async(reader: AsyncQueueReader, sink):
    while True:
        await trio.sleep(random.random() * 0.002)
        m = await reader.recv()
        if not m:
            break
        sink.append(m)


def gen_messages():
    rng = random.Random(SEED)
    return [rng.randbytes(rng.choice(MSG_SIZES)) for _ in range(ROUNDS)]


@pytest.mark.parametrize('delay_prod,delay_cons', [(0, 0)])
def test_sync_roundtrip(delay_prod, delay_cons):
    buf = memoryview(bytearray(BUF_SIZE))
    r, w = create_queue(buf)
    msgs = gen_messages()
    out = []

    tp = threading.Thread(target=producer_sync, args=(w, msgs, delay_prod))
    tc = threading.Thread(target=consumer_sync, args=(r, out, delay_cons))
    tp.start()
    tc.start()
    tp.join()
    tc.join()

    assert out == msgs


def test_wraparound_boundary():
    buf = memoryview(bytearray(BUF_SIZE))
    r, w = create_queue(buf)
    payload = os.urandom(BUF_SIZE - 4)
    out = []

    tp = threading.Thread(target=producer_sync, args=(w, [payload]))
    tc = threading.Thread(target=consumer_sync, args=(r, out))
    tp.start()
    tc.start()
    tp.join()
    tc.join()

    assert out == [payload]


def test_reject_too_large():
    buf = memoryview(bytearray(BUF_SIZE))
    _r, w = create_queue(buf)
    with pytest.raises(ValueError):
        w.write(struct.pack('<I', BUF_SIZE) + os.urandom(BUF_SIZE))


def test_blocking_when_consumer_slow():
    '''
    Consumer sleeps so writer must block repeatedly.

    '''
    buf = memoryview(bytearray(BUF_SIZE))
    r, w = create_queue(buf)
    msgs = gen_messages()
    out = []

    tp = threading.Thread(target=producer_sync, args=(w, msgs, 0))
    tc = threading.Thread(target=consumer_sync, args=(r, out, 0.0005))
    tp.start()
    tc.start()
    tp.join()
    tc.join()
    assert out == msgs


async def test_async_roundtrip():
    buf = memoryview(bytearray(BUF_SIZE))
    r, w = create_async_queue(buf)
    msgs, out = gen_messages(), []

    async with trio.open_nursery() as n:
        n.start_soon(producer_async, w, msgs)
        n.start_soon(consumer_async, r, out)

    assert out == msgs


async def test_async_writer_sync_reader_cross_threads():
    '''
    Trio producer feeds a normal QueueReader living in a background thread.

    '''
    rng = random.Random(SEED)
    msgs = [rng.randbytes(rng.choice(MSG_SIZES)) for _ in range(ROUNDS)]
    out = []

    # Build queue components on a shared token
    buf = memoryview(bytearray(BUF_SIZE))
    token = QueueToken.alloc(buf)
    writer = AsyncQueueWriter(token)
    reader = QueueReader(token)

    def eater():
        while True:
            m = reader.recv()
            if not m:
                break
            out.append(m)

    t = threading.Thread(target=eater)
    t.start()

    # async producer
    for p in msgs:
        await writer.send(p)
    await writer.send(b'')  # sentinel

    await trio.to_thread.run_sync(t.join)

    assert out == msgs
