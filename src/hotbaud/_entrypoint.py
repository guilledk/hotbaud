from functools import partial

import trio
import trio_asyncio
import trio_parallel


def worker_main(
    *,
    worker_id: str,
    task: partial,
    asyncio: bool
) -> None:
    task.keywords['worker_id'] = worker_id
    try:
        if not asyncio:
            trio.run(task)
        else:
            trio_asyncio.run(task)

    except Exception as e:
        e.add_note(f'From worker {worker_id}')
        raise


async def run_in_worker(worker_id: str, task: partial, *, asyncio: bool = False, **kwargs) -> None:
    task = partial(
        worker_main,
        worker_id=worker_id,
        task=task,
        asyncio=asyncio
    )
    async with trio_parallel.open_worker_context(
        **kwargs
    ) as ctx:
        await ctx.run_sync(task, kill_on_cancel=True)


