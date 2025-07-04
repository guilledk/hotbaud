# The MIT License (MIT)
# 
# Copyright © 2025 Guillermo Rodriguez & Tyler Goodlet
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the “Software”), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
'''
Entrypoint/spawning for pipeline workers

`run_in_worker` is used by the root process to spawn a new child with a defined
"task" to run (partial function type can be sync or async, trio or asyncio).

Spawning is done using the `trio.run_process` call, a script entry point is
defined in `hotbaud`'s pyproject.toml named "hotbaud-worker" which points to
the `worker_main` function.

The `worker_main` function expects a `WorkerSpec` struct encoded as json string
to be present in the `HOTBAUD_WORKER_SPEC` envoirment variable, the spec
contains the worker id & all the info necesary on what and how to run the
partial originally passed to `run_in_worker`.

'''

import os
import inspect
import pkgutil

import sys
from typing import Any, Self
from functools import partial

import trio
import trio_asyncio

from hotbaud._utils import MessageStruct, namespace_for


spec_env_var = 'HOTBAUD_WORKER_SPEC'


class TaskSpec(MessageStruct, frozen=True):
    '''
    A functools.partial "codec" to move across process bounds

    '''

    fn: str  # namespace string (valid for pkgutil.resolve_name)
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
        return partial(pkgutil.resolve_name(self.fn), *self.args, **self.kwargs)


class WorkerSpec(MessageStruct, frozen=True):
    '''
    Metadata that worker process needs to run a "task", which can be a
    callable or awaitable (trio or asyncio style)

    '''

    id: str
    task: TaskSpec
    asyncio: bool = False


def worker_main() -> None:
    '''
    Actual sync entrypoint for all workers spawned using `run_in_worker`,
    expect a json encoded `WorkerSpec` to be on `HOTBAUD_WORKER_SPEC`
    environment variable which contains all info to run the target function
    (which can be a callable or an awaitable, trio or asyncio style).

    '''
    encoded_spec = os.environ.get(spec_env_var, None)
    if not encoded_spec:
        raise RuntimeError(
            'Looks like you ran a worker entry point without the'
            f' "{spec_env_var}" environment variable set (should contain json'
            ' encoded WorkerSpec)'
        )

    # parse spec
    spec = WorkerSpec.from_json(encoded_spec)

    # unpack to actual partial
    task = spec.task.to_partial()

    # inject worker_id keyword arg from spec id
    task.keywords['worker_id'] = spec.id

    if spec.task.is_async:
        if not spec.asyncio:
            trio.run(task)
        else:
            trio_asyncio.run(task)

    else:
        task()


async def run_in_worker(
    worker_id: str, task: partial, *, asyncio: bool = False, **kwargs
) -> None:
    '''
    Run a callable or awaitable (trio or asyncio style) function inside a
    separate process.

    TODO:

     - automatically create a "control channel" to send cancel, get a return
     value, events etc.

    '''
    # pack everything we need into env var
    spec = WorkerSpec(
        id=worker_id, task=TaskSpec.from_partial(task), asyncio=asyncio
    )

    env = os.environ.copy()
    env[spec_env_var] = spec.encode_str()

    # spawn the child
    cmd = [
        sys.executable,
        '-m',
        'hotbaud.experimental._worker',
        worker_id,  # just so worker id is shown on process info
    ]

    process: trio.Process = await trio.lowlevel.open_process(cmd, env=env, **kwargs)

    try:
        await process.wait()

    finally:
        # if process still running, attempt best-effort cleanup
        if process.returncode is None:
            process.terminate()


if __name__ == '__main__':
    worker_main()
