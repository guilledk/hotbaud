import inspect
import importlib
from functools import partial
from types import ModuleType
from typing import Callable, Self

import msgspec

from hotbaud.types import Buffer


class MessageStruct(msgspec.Struct, frozen=True):
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


class InternalError(Exception): ...


def disable_resource_tracker():
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


class ResolveError(RuntimeError):
    '''
    Could not resolve the given namespace string into a callable.

    '''


def resolve_callable(path: str) -> Callable[..., object]:
    '''
    Return the callable referred to by *path*.

    The syntax is either

        "pkg.module:attr.subattr"
        "pkg.module.attr.subattr"

    where everything **left of the first ":" (if present)** or the
    **last “.”** is treated as the importable module, and the remainder
    is traversed with successive ``getattr`` calls.

    Raises
    ------
    ResolveError
        If the module cannot be imported, an attribute is missing, or the
        final object is not callable.

    '''
    if ':' in path:
        module_path, attr_path = path.split(':', 1)
    else:
        module_path, attr_path = path.rsplit('.', 1)

    # 1. import the root module
    try:
        module: ModuleType = importlib.import_module(module_path)
    except Exception as exc:  # pragma: no cover
        raise ResolveError(f"Cannot import module '{module_path}'") from exc

    # 2. walk the attribute chain
    obj: object = module
    for segment in attr_path.split('.'):
        try:
            obj = getattr(obj, segment)
        except AttributeError as exc:  # pragma: no cover
            raise ResolveError(
                f"Module '{module_path}' has no attribute chain "
                f"'{attr_path}' (failed at '{segment}')"
            ) from exc

    # 3. make sure we ended with something callable
    if not callable(obj):
        raise ResolveError(
            f"Resolved object '{module_path}:{attr_path}' is not callable"
        )

    return obj


class PathError(RuntimeError):
    '''
    The given object cannot be expressed as an importable namespace path.

    '''


def _root_object(obj: Callable) -> Callable:
    '''
    Peel away functools.wraps / decorators so we record the *real* function.

    '''
    return inspect.unwrap(obj)


def namespace_for(obj: Callable, *, use_colon: bool = True) -> str:
    '''
    Return an import-path string for *obj* that round-trips with
    ``resolve_callable`` -> ``namespace_for``.

    Examples
    --------
    >>> namespace_for(math.sqrt)
    'math:sqrt'
    >>> namespace_for(PathLike.__fspath__)  # a classmethod
    'os:pathlib.PathLike.__fspath__'

    Notes & Caveats
    ---------------
    * The function **must** be defined at module scope (or as a method of such
      a class).  Nested/closure functions do *not* have a stable import path.
    * Objects defined in ``__main__`` cannot be resolved after a fork/exec.
    * Decorated callables are unwrapped first so you get the real origin.

    '''
    obj = _root_object(obj)

    # 1. We need a reliable module.
    mod_name = getattr(obj, '__module__', None)
    if not mod_name or mod_name == '__main__':
        raise PathError('Object sits in __main__; no import path exists')

    qual = getattr(obj, '__qualname__', None)
    if not qual:
        raise PathError('Object lacks __qualname__')

    # 2. Validate that the final hop really is reachable via getattr-chains.
    here = importlib.import_module(mod_name)
    for segment in qual.split('.'):
        try:
            here = getattr(here, segment)
        except AttributeError as exc:  # pragma: no cover
            raise PathError(
                f"{mod_name}:{qual} cannot be resolved (failed at '{segment}')"
            ) from exc

    # 3. Ensure round-trip identity (useful with decorators).
    if _root_object(here) is not obj:
        raise PathError(
            f'Round-trip mismatch for {obj!r} - '
            f'got {_root_object(here)!r} instead'
        )

    sep = ':' if use_colon else '.'
    return f'{mod_name}{sep}{qual}'
