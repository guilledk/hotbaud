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
import sys
import logging
import inspect
import pkgutil

from typing import Any, Callable, Self
from logging import Logger
from functools import partial
from contextlib import suppress

import trio
import trio_asyncio
import msgspec

from hotbaud._utils import MessageStruct, namespace_for

# eventualy move hotbaud.experimental.event into hotbaud.event and stop using
# relative import
from .event import Event


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
    exit_fd: int | None
    asyncio: bool = False
    config: dict | None = None
    config_type: str | None = None
    log_setup: str | None = None

    def unwrap_config(self) -> dict | msgspec.Struct | None:
        if not self.config or not self.config_type:
            return None

        if self.config_type == 'dict':
            return self.config

        cfg_type = pkgutil.resolve_name(self.config_type)
        return msgspec.convert(self.config, type=cfg_type)

    def setup_log(self) -> Logger:
        if self.log_setup:
            log_setup_fn = pkgutil.resolve_name(self.log_setup)
            log_setup_fn()

        return logging.getLogger(self.id)


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

    # maybe run user's log setup & get worker logger
    log = spec.setup_log()
    task.keywords['log'] = log

    # inject worker_id keyword arg from spec id
    task.keywords['worker_id'] = spec.id

    # mabye inject config
    if (config := spec.unwrap_config()):
        task.keywords['config'] = config

    # maybe open exit event:
    # depending on the stage this worker belongs to it might need to wait on
    # next stage's exit `Event` in order to keep channel resources alive while
    # next stage hasnt exited, last stage workers dont need to wait so their
    # exit_fd is None
    exit_event = None
    if spec.exit_fd:
        exit_event = Event(spec.exit_fd)

    log.info('starting...')

    try:
        # finally run unwrapped user task, choose right runtime based on params
        if spec.task.is_async:
            if not spec.asyncio:
                trio.run(task)
            else:
                trio_asyncio.run(task)

        else:
            task()

    except Exception as e:
        # on errors we append worker id info and just raise
        e.add_note(f'with <3 from worker {spec.id}')
        raise

    finally:
        if exit_event:
            # this worker belongs to a stage other than the last, wait next
            # stage's workers to exit before exiting ourselves
            log.info('waiting on prev stage exit event...')

            # attempt waiting on prev stage exit event capture error on failure
            # with suppress(Exception) as e:
            try:
                exit_event.wait()
                log.info('received exit event, stopping...')

            except Exception as e:
                # failed to wait on exit event, likely broken fd?
                # just log warning and exit, we dont want to hide any real
                # errors
                log.warning(
                    'failed read on exit event, stopping...',
                    exc_info=e
                )

        else:
            # if we are last step of pipeline we dont need to wait on any exit
            # event
            log.info('exiting...')


async def run_in_worker(
    worker_id: str,
    task: partial,
    exit_fd: int | None,
    *,
    asyncio: bool = False,
    config: dict | msgspec.Struct | None,
    log_setup: Callable[[], None] | None,
    **kwargs
) -> None:
    '''
    Run a callable or awaitable (trio or asyncio style) function inside a
    separate process.

    TODO:

     - automatically create a "control channel" to send cancel, get a return
     value, events etc.

    '''
    config_type: str | None = None
    config_dict: dict | None = None
    if config is not None:
        # pass config dict or struct as well as info to re-build it if its a struct
        config_type = (
            'dict'
            if isinstance(config, dict)
            else namespace_for(type(config))
        )
        config_dict = (
            config
            if isinstance(config, dict)
            else msgspec.to_builtins(config)
        )

    # also pass namespace of log_setup function if user passed it
    log_setup_ns: str | None = namespace_for(log_setup) if log_setup else None

    # pack everything we need into env var
    spec = WorkerSpec(
        id=worker_id,
        task=TaskSpec.from_partial(task),
        exit_fd=exit_fd,
        config=config_dict,
        config_type=config_type,
        asyncio=asyncio,
        log_setup=log_setup_ns
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

    # TODO: pass exit_fd though our own medium (hotbaud._fdshare)
    if exit_fd is not None:
        if 'pass_fds' not in kwargs:
            kwargs['pass_fds'] = []

        kwargs['pass_fds'].append(exit_fd)

    process: trio.Process = await trio.lowlevel.open_process(
        cmd, env=env, **kwargs
    )

    try:
        await process.wait()

    finally:
        # if process still running, attempt best-effort cleanup
        if process.returncode is None:
            process.terminate()


if __name__ == '__main__':
    worker_main()
