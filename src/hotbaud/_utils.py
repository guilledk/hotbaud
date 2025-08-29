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
Misc utils that don't deserve their own module (yet)

'''

from contextlib import contextmanager

import os
import threading
import time
import signal
import inspect

from types import ModuleType
from typing import Any, Self
from functools import partial

import psutil
import msgspec

from hotbaud.types import Buffer


class MessageStruct(msgspec.Struct, frozen=True):
    '''
    Base msgspec struct with some common conversion helpers

    '''

    @classmethod
    def from_msg(cls, msg: dict | Self) -> Self:
        if isinstance(msg, cls):
            return msg

        return msgspec.convert(msg, type=cls)

    @classmethod
    def from_bytes(cls, buf: Buffer) -> Self:
        return msgspec.msgpack.decode(buf, type=cls)

    @classmethod
    def from_json(cls, s: Buffer | str) -> Self:
        return msgspec.json.decode(s, type=cls)

    def encode(self, module: str = 'msgpack') -> bytes:
        return getattr(msgspec, module).encode(self)

    def encode_str(self) -> str:
        return msgspec.json.encode(self).decode()

    def to_dict(self) -> dict:
        return msgspec.to_builtins(self)


def disable_resource_tracker():
    '''
    When using resources like `SharedMemory` from std mp lib a
    "resource tracker" system is ran in the bg, and reports lots of false
    resource leaks, specially on structured concurrent envoirments where the
    resource lifecycles are guaranteed to be handled automatically by the use
    of context managers and try..finally clauses.

    This is a function that disables the resource tracker by monkey pathing it
    with a dummy class that does nothing.

    On 3.13 the new `track` keyword argument was added to `SharedMemory` which
    provides a way to opt out of the resource tracker system.

    '''
    from multiprocessing import resource_tracker

    class DummyTracker(resource_tracker.ResourceTracker):
        def register(self, name, rtype):
            pass

        def unregister(self, name, rtype):
            pass

        def ensure_running(self):
            pass

    resource_tracker._resource_tracker = DummyTracker()
    resource_tracker.register = resource_tracker._resource_tracker.register
    resource_tracker.ensure_running = (
        resource_tracker._resource_tracker.ensure_running
    )
    resource_tracker.unregister = resource_tracker._resource_tracker.unregister
    resource_tracker.getfd = resource_tracker._resource_tracker.getfd


def make_partial(fn, /, *args, **kwargs):
    '''
    Wrap fn into a partial, merging with existing partial if needed

    '''
    if isinstance(fn, partial):
        base = fn
        merged_args = (*base.args, *args)
        merged_kwargs = {**(base.keywords or {}), **kwargs}
        return partial(base.func, *merged_args, **merged_kwargs)

    return partial(fn, *args, **kwargs)


def namespace_for(obj: Any) -> str:
    '''
    Given an object like a module or function, return a "namespace" string
    compatible with stdlib's `pkgutil.resolve_name`:

    > It is expected that name will be a string in one of the following formats,
    where W is shorthand for a valid Python identifier and dot stands for a
    literal period in these pseudo-regexes:

    > W(.W)*

    > W(.W)*:(W(.W)*)?

    > The first form is intended for backward compatibility only. It assumes
    that some part of the dotted name is a package, and the rest is an object
    somewhere within that package, possibly nested inside other objects.
    Because the place where the package stops and the object hierarchy starts
    can’t be inferred by inspection, repeated attempts to import must be done
    with this form.

    > In the second form, the caller makes the division point clear through
    the provision of a single colon: the dotted name to the left of the colon
    is a package to be imported, and the dotted name to the right is the object
    hierarchy within that package. Only one import is needed in this form. If
    it ends with the colon, then a module object is returned.

    (from: https://docs.python.org/3/library/pkgutil.html)

    Considerations:

    # Module-level callables are safe:
    Functions, classes, and module attributes that live directly in an
    importable module work fine (this is the main use case).

    # Bound methods are not importable
    Passing instance.method gives you a method object whose attribute path is
    no longer addressable (Path.home lives in the class, but Path().home does
    not).

    >>> namespace_for(Path.home)  # OK
    'pathlib:Path.home'
    >>> namespace_for(Path().home)  # ValueError (not addressable)

    # Nested / local / lambda functions break the round-trip
    Their __qualname__ contains <locals> and there is no corresponding attribute
    chain in the module. The resulting string will fail when you feed it to
    resolve_name.

    # Objects defined in __main__ or interactive sessions
    inspect.getmodule(obj) returns __main__; that module can't be re-imported,
    so the string is useless outside the current interpreter.

    # Decorators that hide the wrapped object
    If a decorator does not copy __module__ and __qualname__ with
    functools.wraps, the helper will produce a path for the wrapper, not the
    original function.

    # Dynamically generated functions
    Anything created with types.FunctionType, eval, or exec may have the right
    __module__, but unless you also assign it into that module's namespace the
    attribute lookup will fail.

    # staticmethod / classmethod
    These descriptor objects themselves are not importable, but their
    underlying function is. Always pass MyCls.method.__func__ (or MyCls.method
    before attribute access turns it into a function/method).

    # Coroutine and generator objects
    The function that returns them is fine; the object you get after calling
    the function (async def f(): ...; f() or def g(): yield) is not importable.

    # functools.partial, operator.methodcaller, other wrappers
    They are plain instances with __call__; no module/qualname path exists.

    # Callable instances of user classes
    A class that implements __call__ (e.g. a functor) resolves to that instance,
    not to a named attribute in the module.

    # Built-ins and C-implemented functions
    Most live in the builtins or math, itertools, ... modules and work
    (builtins:int, itertools:chain). A few low-level callables (e.g. some
    CPython internals) do not expose __qualname__ and therefore fail.

    # Objects removed or renamed after you serialize the path
    The round-trip only works as long as the module’s public surface remains
    unchanged.

    '''
    if isinstance(obj, ModuleType):
        return obj.__name__  # e.g. 'pathlib'

    mod = inspect.getmodule(obj)
    if mod is None:  # built-ins, C-impl, etc.
        raise ValueError('object is not tied to an importable module')

    qual = getattr(obj, '__qualname__', obj.__name__)

    return f'{mod.__name__}:{qual}'  # e.g. 'pathlib:Path.home'


_reaper_enabled = False


@contextmanager
def oom_self_reaper(kill_at_pct: float = 0.7):
    '''
    Ensure the process doesnt eat up all memory

    Launched a bg thread that kills the entire process group if more than
    `kill_at_pct` of system memory is consumed by the process.

    '''
    global _reaper_enabled
    if _reaper_enabled:
        yield
        return

    # compute absolute RSS limit (kill_at_pct% of total RAM)
    limit = int(psutil.virtual_memory().total * kill_at_pct)
    # ensure we’re the leader of our own process‐group
    os.setsid()

    def watchdog():
        me = psutil.Process(os.getpid())
        while True:
            try:
                mem = me.memory_info().rss
                if mem > limit:
                    # kill the entire group we created above
                    os.killpg(os.getpgrp(), signal.SIGKILL)
                    print(
                        'process group killed by cap_memory fixture!\n'
                        f'had {mem:,} bytes in use and configured limit is '
                        f'{limit:,} bytes'
                    )
                time.sleep(0.1)
            except Exception:
                break

    # start background monitor thread (daemon so it dies with the process)
    _reaper_enabled = True
    t = threading.Thread(target=watchdog, daemon=True)
    t.start()
    yield
