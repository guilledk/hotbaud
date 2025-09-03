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

import os
import re
import time
import math
import signal
import random
import inspect
import threading
import collections.abc as cabc

from types import ModuleType
from typing import (
    Annotated,
    Any,
    AsyncGenerator,
    Iterable,
    Mapping,
    Self,
    Sequence,
    get_args,
    get_origin,
    get_type_hints,
)
from functools import partial
from dataclasses import dataclass, field
from contextlib import asynccontextmanager, contextmanager

import anyio
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


'''
Helpers for automatically coercing partial arguments into msgspec Structs
if applicable based on the function definition annotations,
used in .experimental._worker task spec functions.

'''


def _unwrap_annot(tp: Any | None) -> Any | None:
    if tp is None:
        return None
    while get_origin(tp) is Annotated:
        tp = get_args(tp)[0]
    return tp


def _struct_cls(tp: Any) -> type[msgspec.Struct] | None:
    tp = _unwrap_annot(tp)
    if not inspect.isclass(tp):
        return None
    return tp if issubclass(tp, msgspec.Struct) else None


def _seq_elem_if_struct(tp: Any) -> type[msgspec.Struct] | None:
    tp = _unwrap_annot(tp)
    origin = get_origin(tp)
    if origin in (list, Sequence):
        args = get_args(tp)
        if len(args) == 1:
            return _struct_cls(args[0])
    return None


def _varpos_elem_ann(tp: Any) -> Any | None:
    tp = _unwrap_annot(tp)
    origin = get_origin(tp)
    if origin is tuple:
        args = get_args(tp)
        if len(args) == 2 and args[1] is Ellipsis:
            return args[0]
    if origin in (list, Sequence):
        args = get_args(tp)
        if len(args) == 1:
            return args[0]
    return None


def _varkw_value_ann(tp: Any) -> Any | None:
    tp = _unwrap_annot(tp)
    origin = get_origin(tp)
    if origin in (dict, Mapping):
        args = get_args(tp)
        if len(args) == 2:
            return args[1]
    return None


def _convert_by_ann(val: Any, ann: Any) -> Any:
    cls = _struct_cls(ann)
    if cls is not None:
        return msgspec.convert(val, type=cls)
    elem = _seq_elem_if_struct(ann)
    if elem is not None:
        return msgspec.convert(val, type=list[elem])
    return val


def coerce_msgspec_bound_args(p: partial) -> partial:
    '''
    Given a partial, coerce any argument whose type hint annotation implies a
    msgspec.Struct derived type, which we can use msgspec.convert on, and
    return the new partial with coerced args.

    '''
    f = p.func
    sig = inspect.signature(f)
    try:
        hints = get_type_hints(f, include_extras=True)
    except TypeError:
        hints = get_type_hints(f)

    pos_names = [
        prm.name
        for prm in sig.parameters.values()
        if prm.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]
    var_pos_name = next(
        (
            n
            for n, prm in sig.parameters.items()
            if prm.kind == inspect.Parameter.VAR_POSITIONAL
        ),
        None,
    )
    var_kw_name = next(
        (
            n
            for n, prm in sig.parameters.items()
            if prm.kind == inspect.Parameter.VAR_KEYWORD
        ),
        None,
    )

    new_args = list(p.args)
    fixed = min(len(new_args), len(pos_names))
    for i in range(fixed):
        ann = hints.get(pos_names[i])
        if ann is not None:
            new_args[i] = _convert_by_ann(new_args[i], ann)

    if var_pos_name and len(new_args) > len(pos_names):
        elem_ann = _varpos_elem_ann(hints.get(var_pos_name))
        if elem_ann is not None:
            for j in range(len(pos_names), len(new_args)):
                new_args[j] = _convert_by_ann(new_args[j], elem_ann)

    new_kw = dict(p.keywords or {})
    param_names = set(sig.parameters)

    for k, v in list(new_kw.items()):
        if k in param_names:
            ann = hints.get(k)
            if ann is not None:
                new_kw[k] = _convert_by_ann(v, ann)
        elif var_kw_name:
            val_ann = _varkw_value_ann(hints.get(var_kw_name))
            if val_ann is not None:
                new_kw[k] = _convert_by_ann(v, val_ann)

    return partial(f, *new_args, **new_kw)


'''
Out Of Memory self-reaping machinery

'''

# can only enable once per process group
_reaper_enabled = False


@contextmanager
def oom_self_reaper(kill_at_pct: float = 0.7):
    '''
    Ensure the process doesnt eat up all memory

    Launched a bg thread that kills the entire process group if more than
    `kill_at_pct` of system memory is consumed by the process.

    '''
    # prefer module scoped cache
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


@contextmanager
def maybe_oom_self_reaper(kill_at_pct: float | None = None):
    '''
    Optional version of reaper ctx manager, useful for situations where we
    make reaper use configurable.

    '''
    if kill_at_pct is None:
        yield
        return

    with oom_self_reaper(kill_at_pct):
        yield


'''
Disk IO usage monitor

'''


_root_dev_re = re.compile(r'^(?P<root>.+?)(?:p?\d+)?$')


def _root_dev_name(dev_path_or_name: str) -> str:
    name = os.path.basename(dev_path_or_name)
    m = _root_dev_re.match(name)
    return m.group('root') if m else name


def resolve_devices_for_paths(paths: Iterable[str]) -> set[str]:
    '''Map paths to underlying root device names (psutil.perdisk keys).'''
    parts = psutil.disk_partitions(all=True)
    norm = [(dp, dp.mountpoint.rstrip('/') + '/') for dp in parts]

    devs: set[str] = set()
    for p in map(os.path.abspath, paths):
        cands = [dp for dp, mp in norm if p.startswith(mp)]
        if not cands:
            continue
        best = max(
            cands,
            key=lambda dp: len(dp.mountpoint.rstrip('/') + '/'),
        )
        devs.add(_root_dev_name(best.device))
    return devs


@dataclass
class DiskIOGauge:
    sample: float = 0.20
    ema_alpha: float = 0.35
    devices: set[str] | None = None
    _ema: float = 0.0
    _inst: float = 0.0
    _version: int = 0
    _cond: anyio.Condition = field(default_factory=anyio.Condition)
    _prev_busy: dict[str, int] = field(default_factory=dict)
    _prev_ts: float = 0.0

    def snapshot(self) -> float:
        '''Return smoothed utilization in [0.0, 1.0].'''
        return self._ema

    async def wait_below(
        self,
        *,
        high: float,
        low: float | None = None,
        timeout: float | None = None,
        jitter: float = 0.15,
    ) -> None:
        '''Wait until EMA <= high, with optional hysteresis to low.'''
        low = max(0.0, high - 0.20) if low is None else low

        if timeout is None:
            async with self._cond:
                while self._ema > high:
                    await self._cond.wait()
                while self._ema > low:
                    await self._cond.wait()
        else:
            with anyio.move_on_after(timeout) as scope:
                async with self._cond:
                    while self._ema > high:
                        await self._cond.wait()
                    while self._ema > low:
                        await self._cond.wait()
            if scope.cancelled_caught:
                return

        if jitter > 0:
            await anyio.sleep(random.uniform(0.0, jitter))

    async def _tick(self) -> None:
        per = psutil.disk_io_counters(perdisk=True)
        ts = time.perf_counter()

        if not self._prev_busy:
            self._prev_busy = {
                k: v.busy_time
                for k, v in per.items()
                if hasattr(v, 'busy_time')
            }
            self._prev_ts = ts
            return

        dt = max(1e-6, ts - self._prev_ts)
        prev = self._prev_busy
        cur_busy = {
            k: v.busy_time for k, v in per.items() if hasattr(v, 'busy_time')
        }

        keys = set(cur_busy)
        if self.devices:
            keys &= self.devices
        if not keys:
            keys = set(cur_busy)

        total_busy_ms = sum(
            max(0.0, cur_busy.get(k, 0) - prev.get(k, 0)) for k in keys
        )
        denom = dt * 1000.0 * max(1, len(keys))
        inst = min(1.0, total_busy_ms / denom)
        self._inst = inst

        self._ema = self.ema_alpha * inst + (1 - self.ema_alpha) * self._ema

        self._prev_busy, self._prev_ts = cur_busy, ts
        self._version += 1
        async with self._cond:
            self._cond.notify_all()

    async def run(
        self,
        *,
        task_status=anyio.TASK_STATUS_IGNORED,
    ) -> None:
        '''Background loop; cancel the task group to stop.'''
        try:
            await self._tick()
        except Exception:
            pass
        task_status.started(self)
        while True:
            await anyio.sleep(self.sample)
            try:
                await self._tick()
            except Exception:
                pass


@asynccontextmanager
async def open_io_gauge(
    *,
    sample: float = 0.20,
    ema_alpha: float = 0.35,
    paths: Iterable[str] | None = None,
) -> AsyncGenerator[DiskIOGauge, None]:
    '''Start/stop the sampler as a context manager.'''
    devices = resolve_devices_for_paths(paths or []) or None
    gauge = DiskIOGauge(sample=sample, ema_alpha=ema_alpha, devices=devices)
    async with anyio.create_task_group() as tg:
        await tg.start(gauge.run)
        try:
            yield gauge
        finally:
            tg.cancel_scope.cancel()


'''
Simple throughput measurer tool, with scrolling window and ~tau avg

'''


def _now() -> float:
    return time.perf_counter()


@dataclass
class ThroughputMeter:
    '''
    Smooth 'last ~tau' ticks/sec via an exponential time-decay accumulator.

    Idea:
      Maintain X(t) = decayed count of events.
      On record(n) at time t:
         X <- X * exp(-(t - t_prev)/tau) + n
      Instantaneous rate estimate:
         r_hat(t) = X(t) / tau

    - No window edges => much less jumpy than a hard 5s window.
    - Naturally and smoothly decays to 0 during idle periods.
    '''

    tau_seconds: float = 5.0  # “last ~5s” feel
    start: float = field(default_factory=_now)
    last_t: float = field(default_factory=_now)
    X: float = 0.0  # decayed event mass
    total_ticks: int = 0

    def _decay_to(self, t: float) -> None:
        dt = max(0.0, t - self.last_t)
        if dt > 0:
            decay = math.exp(-dt / self.tau_seconds)
            self.X *= decay
            self.last_t = t

    def record(self, n_ticks: int, t: float | None = None) -> None:
        '''
        Store one sample.

        '''
        t = _now() if t is None else t
        self._decay_to(t)
        self.X += float(n_ticks)
        self.total_ticks += int(n_ticks)

    @property
    def rate(self) -> float:
        '''
        Smoothed 'last ~tau' rate in ticks/sec.

        '''
        self._decay_to(_now())
        return self.X / self.tau_seconds

    @property
    def rate_global(self) -> float:
        '''
        Global ticks/sec since start.

        '''
        elapsed = max(1e-9, _now() - self.start)
        return self.total_ticks / elapsed
