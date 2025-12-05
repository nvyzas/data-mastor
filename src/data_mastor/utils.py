import random
import traceback
from collections.abc import Callable, Mapping, Sequence, Set
from enum import StrEnum
from functools import partial
from inspect import Parameter, Signature, signature
from types import ModuleType
from typing import Any, Generic, TypeVar
from unittest.mock import MagicMock, Mock

from click.testing import Result

# CALLABLES


def sigpart(func: Callable, *picked: str):
    sig = signature(func)
    if len(picked) == 0:
        return sig
    picked_params = {k: v for k, v in sig.parameters.items() if k in picked}
    return Signature(list(picked_params.values()))


type clb_or_sig = Sequence[Callable | Signature]


def replace_function_signature(
    func: Callable,
    other: Callable | Signature | Sequence[Callable | Signature],
    no_variadic=False,
    excluded_params: Sequence[str] | Mapping[str, clb_or_sig] | None = None,
    edit_annotations=True,
    edit_name=False,
) -> Callable[..., Any]:
    # get parameters
    params: dict[str, Parameter] = {}
    if not isinstance(other, Sequence):
        other = [other]
    if excluded_params is None:
        excluded_params = []
    if isinstance(excluded_params, Sequence):
        excluded_params = {k: other for k in excluded_params}
    for o in other:
        sig = o if isinstance(o, Signature) else signature(o)
        sig_params = {}
        for k, v in sig.parameters.items():
            if k in excluded_params and o in excluded_params[k]:
                continue
            sig_params[k] = v
        params.update(sig_params)

    # remove variadic
    if no_variadic:
        to_del = [
            k for k, v in params.items() if v.kind in [v.VAR_POSITIONAL, v.VAR_KEYWORD]
        ]
        for k in to_del:
            del params[k]

    # sort parameters by kind
    order = {
        Parameter.POSITIONAL_ONLY: 0,
        Parameter.POSITIONAL_OR_KEYWORD: 1,
        Parameter.VAR_POSITIONAL: 2,
        Parameter.KEYWORD_ONLY: 3,
        Parameter.VAR_KEYWORD: 4,
    }
    sorted_params = dict(sorted(params.items(), key=lambda item: order[item[1].kind]))

    # edit signature
    func.__signature__ = Signature(list(sorted_params.values()))  # type: ignore

    # edit annotations
    if edit_annotations:
        annots = {name: param.annotation for name, param in sorted_params.items()}
        func.__annotations__ = annots

    # edit name
    if edit_name:
        func.__name__ = "__".join(
            ["sig" if isinstance(o, Signature) else o.__name__ for o in other]
        )
    return func


def combine_funcs(
    funcs: Sequence[Callable], kwargs_updater: Callable | None = None, verbose=False
) -> Callable[..., None]:
    if not funcs:
        raise ValueError(f"Sequence of functions argument seems empty ({funcs})")

    def combined(**kwargs):
        if verbose:
            print("\nCombined: start")
        if kwargs_updater is not None:
            params = signature(kwargs_updater).parameters
            kw = {k: v for k, v in kwargs.items() if k in params}
            if verbose:
                print(f"Combined: calling '{kwargs_updater.__name__}' with: {kw}")
            updates = kwargs_updater(**kw)
            kwargs.update(updates)
        for f in funcs:
            params = signature(f).parameters
            kw = {k: v for k, v in kwargs.items() if k in params}
            if verbose:
                print(f"Combined: calling '{f.__name__}' with: {kw}")
            f(**kw)
        if verbose:
            print("Combined: end\n")

    updater = [kwargs_updater] if kwargs_updater else []
    replace_function_signature(combined, [*updater, *funcs], no_variadic=True)
    return combined


# TESTING
# REF use a single configurable class as a factory


def _different(set: Set | Mapping) -> int:
    while True:
        i = random.randint(555, 999)
        if i not in set:
            return i


class MockFactory:
    def __init__(self, mocks: dict[str, Mock] | None = None) -> None:
        self._mocks = {} if mocks is None else mocks

    def __call__(
        self,
        id_: Callable | str | int | None = None,
        mockcls: type[Mock] = MagicMock,
        **kwargs,
    ) -> Mock:
        if id_ is None:
            id_ = _different(self._mocks)
        elif callable(id_):
            # try:
            #     obj_id =
            # except AttributeError:
            #     raise
            return self._mocks[id_.__name__]

        id_ = str(id_)
        if (m := self._mocks.get(id_)) is not None:
            return m
        mock = mockcls(**kwargs, name=id_)
        mock.__name__ = id_
        self._mocks[id_] = mock
        return mock


class FunctionFactory:
    def __init__(
        self,
        funcs: dict[str, Callable] | None = None,
        mocks: dict[str, Mock] | None = None,
    ) -> None:
        self._funcs = {} if funcs is None else funcs
        self.mf = MockFactory(mocks)

    def __call__(
        self,
        id_: str | int | None = None,
        func: Callable | None = None,
        sig: Callable | Signature | None = None,
        **mock_kwargs,
    ) -> Callable:
        if id_ is None:
            id_ = _different(self._funcs)

        id_ = str(id_)
        if (f := self._funcs.get(id_)) is not None:
            return f

        mock = self.mf(id_, **mock_kwargs)

        def _f(*args, **kwargs):
            print(f"Running (mock-containing) function '{id_}' with {kwargs}")
            mock(*args, **kwargs)
            if func:
                kw = {
                    k: v for k, v in kwargs.items() if k in signature(func).parameters
                }
                func(*args, **kw)
            return id_

        if sig:
            replace_function_signature(_f, [sig])
        elif func:
            replace_function_signature(_f, [func])

        _f.__name__ = id_
        self._funcs[id_] = _f
        return _f


def patch_path(obj: Callable, at: Any | ModuleType | str | None = None) -> str:
    lookup_path = (
        obj.__module__
        if at is None
        else at
        if isinstance(at, str)
        else at.__name__
        if isinstance(at, ModuleType)
        else at.__module__
    )
    return lookup_path + "." + obj.__name__


class _Sentinel:
    pass


class Assertion:
    """Provides a way to declare an assertion in two stages: using a known object at init time,
    and an unknown one later. This can help, for example, when declaring testcases.
    """

    # assertion methods (from specific to generic)
    @staticmethod
    def assert_type(type_: type, obj: type) -> None:
        if isinstance(obj, type):
            assert issubclass(obj, type_)
        else:
            assert isinstance(obj, type_)

    @staticmethod
    def assert_exceptions_equal(
        exc1: BaseException, exc2: BaseException, exact_type=True, exact_message=False
    ) -> None:
        if exact_type:
            assert type(exc2) is type(exc1)
        else:
            assert issubclass(type(exc2), type(exc1))
        if exact_message:
            assert exc2.args[0] == exc1.args[0]
        else:
            assert exc2.args[0] in exc1.args[0] or exc1.args[0] in exc2.args[0]

    @staticmethod
    def assert_func_returns(func: Callable, *args, true_only=False, **kwargs) -> None:
        if true_only:
            assert func(*args, **kwargs)
        else:
            func(*args, **kwargs)

    # DO (COP) implement strict_subset, dict_only
    @staticmethod
    def assert_attributes(
        attrs: dict[str, Any],
        obj,
        ignore_values=False,
        strict_subset=False,
        dict_only=False,
    ) -> None:
        for k, v in attrs.items():
            assert getattr(obj, k) == v

    @staticmethod
    def assert_value(obj1, obj2, identity_check=False):
        if identity_check:
            assert obj1 is obj2
        else:
            assert obj1 == obj2

    # category: concerns the known object of the assertion (from specific to generic)
    class Category(StrEnum):
        EXCEPTION = "exception"
        TYPE = "type"
        TRUTHFUNC = "truthfunc"
        ATTRIBUTES = "attributes"
        EQUALS = "equals"

    methods: dict[Category, Callable] = {
        Category.EXCEPTION: assert_exceptions_equal,
        Category.TYPE: assert_type,
        Category.TRUTHFUNC: assert_func_returns,
        Category.ATTRIBUTES: assert_attributes,
        Category.EQUALS: assert_value,
    }

    types: dict[Any, Category] = {
        BaseException: Category.EXCEPTION,
        type: Category.TYPE,
        Callable: Category.TRUTHFUNC,
        dict: Category.ATTRIBUTES,
        object: Category.EQUALS,
    }

    def __init__(
        self,
        known: Any,
        category: Category | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> None:
        # parse from tuple
        if isinstance(known, tuple) and category is None and kwargs is None:
            if len(known) == 2:
                known, category = known
            elif len(known) == 3:
                known, category, kwargs = known

        # set known object
        self.known = known

        # set or validate category (of known object)
        category = None if category is None else self.Category(category)
        for k, v in self.types.items():
            if isinstance(self.known, k):
                if category is None:
                    category = v
                elif category not in [v]:
                    raise ValueError(f"{category} does not support value of {known}")
                break
        else:
            raise RuntimeError(f"Couldn't determine category of {known}")
        self.category = category

        # set kwargs
        self.kwargs = {} if kwargs is None else kwargs

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        m = self.methods[self.category]
        partial(m, self.known, **self.kwargs)(*args, **kwargs)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Assertion):
            return NotImplemented
        return self.__dict__ == other.__dict__


R = TypeVar("R")


class Outcome(Generic[R]):
    def __init__(self, func: Callable[..., R], *args: Any, **kwargs: Any) -> None:
        self.func: Callable[..., R] = func
        self.args: tuple[Any, ...] = args
        self.kwargs: dict[str, Any] = kwargs
        self._outcome: R | Exception | _Sentinel = _Sentinel()

    @property
    def outcome(self) -> R | Exception:
        if isinstance(self._outcome, _Sentinel):
            raise RuntimeError("No outcome since function has not been fired yet")
        return self._outcome

    def __call__(self) -> "Outcome[R]":
        try:
            self._outcome = self.func(*self.args, **self.kwargs)
        except Exception as exc:
            self._outcome = exc
        return self


# define helper for error detection
def assert_result_exit_code(result: Result, exit_code=0):
    d: dict[str, str] = {}
    d["exit_code"] = str(result.exit_code)
    d["stdout"] = result.stdout
    d["stderr"] = result.stderr
    d["exception"] = str(result.exception)
    e = result.exception
    tb = ""
    if e := result.exception:
        tb = "".join(traceback.format_exception(type(e), value=e, tb=e.__traceback__))
    d["tb"] = tb
    diagnostics = "Diagnostics:\n" + "\n".join([_ for _ in d.values() if _])
    assert result.exit_code == exit_code, diagnostics  # pytest-specific


# MISC


def nested_dict_get(
    dict_: dict[str, Any],
    keys: list[str] | str | None = None,
    trace_unknown_keys: bool = False,
    raise_on_error: bool = True,
    debug_on_error: bool = False,
    expected_ret_cls: Any = dict,
) -> tuple[list[str], Any]:
    if keys is None:
        keys = []
    elif isinstance(keys, str):
        keys = [keys]
    for i, key in enumerate(keys):
        if not isinstance(dict_, dict):
            msg = f"Object under keys {keys[:i]} is not a dictionary"
            if raise_on_error:
                raise TypeError(msg)
            else:
                if debug_on_error:
                    print(f"WARNING: {msg}")
                return keys[:i], expected_ret_cls()
        if key not in dict_.keys():
            msg = f"Dict under keys {keys[:i]} has no key '{key}'"
            if raise_on_error:
                raise KeyError(msg)
            else:
                if debug_on_error:
                    print(f"WARNING: {msg}")
                return keys[:i], expected_ret_cls()
        dict_ = dict_[key]
    if trace_unknown_keys:
        unknown_keys = []
        while True:
            if not isinstance(dict_, dict):
                break
            marked_keys = [k for k in dict_ if "!" in k]
            if len(marked_keys) > 1:
                raise KeyError(f"There are multiple marked keys ({marked_keys})")
            if len(marked_keys) == 0:
                break
            unknown_keys.append(marked_keys[0])
            dict_ = dict_[unknown_keys[-1]]
        keys += unknown_keys
    if not issubclass(type(dict_), expected_ret_cls):
        msg = f"Returned obj ({dict_}) is not of expected class ({expected_ret_cls})"
        if raise_on_error:
            raise TypeError(msg)
        else:
            if debug_on_error:
                print(f"WARNING: {msg}")
            return keys, expected_ret_cls()
    return keys, dict_
