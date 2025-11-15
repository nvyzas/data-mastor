import inspect
from collections.abc import Callable
from functools import wraps
from inspect import Signature, get_annotations, signature
from unittest.mock import MagicMock, Mock

import click
import pytest
from click.testing import Result
from rich import print
from typer import Context, Typer
from typer.testing import CliRunner

calls = []


def add(name):
    calls.append(name)


def cb0(ctx: Context):
    print("Callback 0")
    add(0)


def cb1(ctx: Context):
    print("Callback 1")
    add(1)


def _printresult(result: Result):
    if result.output:
        print("output:")
        print(result.output)
    print("ret:", result.return_value)
    print("exc:", {result.exception})


def check(value):
    assert calls == value
    calls.clear()


def cmd0(s):
    print("Cmd0", s)


def cmd1(s):
    print("Cmd1", s)


@pytest.fixture
def app0() -> Typer:
    app0 = Typer(name="zero", invoke_without_command=True)
    return app0


@pytest.fixture
def app1() -> Typer:
    app1 = Typer(name="one", invoke_without_command=True)
    return app1


def test_auto_invoke(app0: Typer, app1: Typer) -> None:
    print()
    check([])
    # app0 auto-invoke
    app0.callback()(cb0)
    result = CliRunner().invoke(app0)
    _printresult(result)
    check([0])
    # app01
    app1.callback()(cb1)
    app0.add_typer(app1)
    # app01 auto-invoke
    result = CliRunner().invoke(app1, "one")
    _printresult(result)
    check([0, 1])


def app_funcs_from_keys(app: Typer, keys: list[str] | None = None):
    funcs = []

    # look in the root app (first arg)
    # its name should NOT be included in the keys arg
    if not keys:
        if app.registered_callback:
            _f = app.registered_callback.callback
            print(f"Adding callback '{_f}' from root app")
            funcs.append(_f)
        if app.registered_groups:
            msg = "There are registered groups in root but no keys given"
            raise ValueError(msg)
        if len(app.registered_commands) > 1:
            msg = "There are more than one commands in root but no keys given"
            raise ValueError(msg)
        if len(app.registered_commands) == 1:
            _f = app.registered_commands[0].callback
            funcs.append(_f)
        return funcs

    # iterate through keys (since all must be matched)
    tpr = app
    for i, key in enumerate(keys[:-1]):
        # check for callback
        if tpr.registered_callback:
            _f = tpr.registered_commands[0].callback
            print(f"Adding callback '{_f}' from app named '{key}'")
            funcs.append(tpr.registered_callback.callback)
        # traverse groups
        key_tprs = []
        for g in tpr.registered_groups:
            if g.typer_instance is None:
                print(f"WARNING: group {g} named '{g.name}' has no typer instance")
                continue
            if g.name == key or g.typer_instance.info.name == key:
                key_tprs.append(g.typer_instance)
        if len(key_tprs) > 1:
            raise ValueError(f"There are more than one apps named '{key}'")
        if not key_tprs:
            raise ValueError(f"There is no app named '{key}' (keys: {keys})")
        tpr = key_tprs[0]

    # look for the command (using the last key)
    key = keys[-1]
    cmds = [_ for _ in tpr.registered_commands if _.callback is not None]
    key_cmds = [_ for _ in cmds if key in [_.name, _.callback.__name__]]  # type: ignore
    if len(key_cmds) > 1:
        raise ValueError(f"There are more than one commands named '{key}'")
    if not key_cmds:
        raise ValueError(f"No command found (from the last key '{key}')")
    funcs.append(key_cmds[0].callback)
    return funcs

    return funcs


mocks = {}


def ff(id_: str | int) -> Callable:
    """Function Factory (using mocks)"""

    def _printer():
        print(id_)

    nm = f"f{id_}"
    return mocks.setdefault(
        id_, MagicMock(return_value=id_, side_effect=_printer, name=nm, __name__=nm)
    )


def test_cmd_from_keys_single_callback() -> None:
    app0 = Typer()
    app0.callback()(ff(1))
    funcs = app_funcs_from_keys(app0)
    assert funcs == [ff(1)]


def test_cmd_from_keys_single_command() -> None:
    app1 = Typer()
    app1.command()(ff(2))
    funcs = app_funcs_from_keys(app1)
    assert funcs == [ff(2)]


def test_cmd_from_keys_callback_and_command() -> None:
    app2 = Typer()
    app2.callback()(ff(1))
    app2.command()(ff(2))
    funcs = app_funcs_from_keys(app2)
    assert funcs == [ff(1), ff(2)]


def test_cmd_from_keys_nested():
    app1 = Typer()
    app1.callback()(ff(10))
    app1.command()(ff(11))
    app2 = Typer()
    app2.callback()(ff(20))
    app2.command()(ff(21))
    app1.add_typer(app2)
    with pytest.raises(ValueError):
        app_funcs_from_keys(app1)
    # lvl1 command
    assert app_funcs_from_keys(app1, ["f11"]) == [ff(10), ff(11)]
    # lvl2 command
    app2.info.name = "two"
    assert app_funcs_from_keys(app1, ["two", "f21"]) == [ff(10), ff(11), ff(20), ff(21)]
    # assert app_funcs_from_keys(app1, ["f11"]) == [ff(11)]
    # assert app_funcs_from_keys(app1, ["f11"]) == [ff(11)]


def test_cmd_from_keys_flat_with_cmd(app0):
    app0.command("cmd0")(cb0)
    funcs = app_funcs_from_keys(app0, ["zero", "cmd0"])
    assert funcs == [cb0]


def combine(funcs):
    def combined(**kwargs):
        for f in funcs:
            params = signature(f).parameters
            kw = {k: v for k, v in kwargs.items() if k in params}
            print(f"Calling {f.__name__} with: {kw}")
            f(**kw)

    all_names = []
    all_params = {}
    for f in funcs:
        all_names.append(f.__name__)
        all_params.update(signature(f).parameters)
    combined.__name__ = "__".join(all_names)
    combined.__signature__ = Signature(list(all_params.values()))  # type: ignore
    return combined


def assimilate(megafunc):
    def outer(newfunc):
        mega_params = signature(megafunc).parameters
        new_params = signature(newfunc).parameters

        def inner(**kwargs):
            kw = {k: v for k, v in kwargs.items() if k in mega_params}
            print(f"Calling megafunc {megafunc.__name__} with: {kw}")
            megafunc(**kw)
            kw = {k: v for k, v in kwargs.items() if k in new_params}
            print(f"Calling newfunc {newfunc.__name__} with: {kw}")
            newfunc(**kw)

        inner.__name__ = megafunc.__name__ + "__" + newfunc.__name__
        inner.__signature__ = Signature(list((mega_params | new_params).values()))
        inner.__annotations__ = get_annotations(megafunc) | get_annotations(newfunc)
        return inner

    return outer


def f(a=1, b=2, c=3):
    print(f"f: {a},{b},{c}")


def f1(b=4, c=5, d=6):
    print(f"f1: {b},{c},{d}")


def f2(e=7):
    print(f"f2: {e}")


into_f = assimilate(f)


def test_assimilate():
    print()

    z1 = assimilate(f)(f1)
    print(z1.__name__, inspect.signature(z1))
    z1(a=11, b=22, c=33, d=44)
    print()

    z2 = assimilate(z1)(f2)
    print(z2.__name__, inspect.signature(z2))
    z2(a=11, b=22, c=33, d=44, e=55)
    print()
