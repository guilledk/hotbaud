from functools import partial
import inspect
import os
import sys
from typing import Any, Self

import trio
import trio_asyncio

from hotbaud._utils import MessageStruct, namespace_for, resolve_callable


class TaskSpec(MessageStruct, frozen=True):
    fn: str  # namespace string
    args: tuple[Any, ...]
    kwargs: dict[str, Any]

    is_async: bool

    @classmethod
    def from_partial(cls, p: partial) -> Self:
        return cls(
            fn=namespace_for(p.func),
            args=p.args,
            kwargs=p.keywords,
            is_async=inspect.iscoroutinefunction(p.func),
        )

    def to_partial(self) -> partial:
        return partial(resolve_callable(self.fn), *self.args, **self.kwargs)


class WorkerSpec(MessageStruct, frozen=True):
    id: str
    task: TaskSpec
    asyncio: bool = False


def worker_main(spec: WorkerSpec) -> None:
    task = spec.task.to_partial()

    task.keywords['worker_id'] = spec.id
    if spec.task.is_async:
        if not spec.asyncio:
            trio.run(task)
        else:
            trio_asyncio.run(task)

    else:
        task()


async def run_in_worker(
    worker_id: str,
    task: partial,
    *,
    asyncio: bool = False,
    python: str | None = None,
) -> None:
    # 1. Pack everything we need into one env var.
    spec = WorkerSpec(
        id=worker_id, task=TaskSpec.from_partial(task), asyncio=asyncio
    )
    env = os.environ.copy()
    env['HOTBAUD_WORKER_SPEC'] = spec.encode_str()

    # 2. Spawn the child
    cmd = [python or sys.executable, '-m', 'hotbaud._entrypoint']

    async with trio.open_nursery() as nursery:
        process: trio.Process = await nursery.start(partial(trio.run_process, cmd, env=env))

        # 3. Propagate cancellation & errors upstream.
        try:
            await process.wait()

        finally:
            if process.returncode is None:  # still running?
                process.terminate()  # best-effort cleanup


if __name__ == '__main__':
    spec = WorkerSpec.from_json(os.environ.pop('HOTBAUD_WORKER_SPEC'))
    worker_main(spec)
