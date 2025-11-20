import random
from collections.abc import Callable, Collection, Sequence
from inspect import Parameter, Signature, signature
from typing import Any, cast
from unittest.mock import MagicMock


def replace_function_signature(
    func: Callable,
    other_functions: list[Callable],
    no_variadic=False,
    edit_annotations=True,
    edit_name=False,
) -> Callable[..., Any]:
    # get parameters
    params: dict[str, Parameter] = {}
    for f in other_functions:
        sig = signature(cast(Callable[..., Any], f))
        params.update(sig.parameters)

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
        func.__name__ = "__".join([_.__name__ for _ in other_functions])
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


def _different(collection: Collection):
    while True:
        i = random.randint(100, 10000)
        if i not in collection:
            return i


mocks: dict[str | int, Callable] = {}


def mock_function_factory(
    id_: int | str | None = None,
    func: Callable | None = None,
    sigfunc: Callable | None = None,
    **mock_kwargs,
) -> Callable:
    """Function Factory (using mocks)"""
    if id_ is None:
        id_ = _different(mocks)
    elif (mock := mocks.get(id_)) is not None:
        return mock

    name = str(id_)

    mock = MagicMock(**mock_kwargs)
    mocks[id_] = mock

    def _f(**kwargs):
        print(f"Running mock function '{id_}' with {kwargs}")
        mock()
        if func is not None:
            func(**kwargs)
        return id_

    _f.__name__ = name
    sigfrom = sigfunc if sigfunc else func if func else _f
    replace_function_signature(_f, [sigfrom])

    return _f
