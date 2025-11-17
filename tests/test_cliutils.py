import inspect
import random
from collections.abc import Callable
from functools import wraps
from inspect import Signature, get_annotations, signature
from typing import Any, Collection
from unittest.mock import MagicMock, Mock

import click
import pytest
from click.testing import Result
from rich import print
from typer import Context, Typer
from typer.testing import CliRunner

from data_mastor.cliutils import app_funcs_from_keys

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


def _different(collection: Collection):
    while True:
        i = random.randint(100, 10000)
        if i not in collection:
            return i


mocks: dict[str | int, Callable] = {}


def ff(id_: int | None = None) -> Callable:
    """Function Factory (using mocks)"""
    if id_ is None:
        id_ = _different(mocks)
    elif (mock := mocks.get(id_)) is not None:
        return mock

    def _printer() -> None:
        print(id_)

    name = str(id_)
    mock = MagicMock(return_value=id_, side_effect=_printer, name=name, __name__=name)
    mocks[id_] = mock
    return mock


class Tf:
    """(Typer) App Factory"""

    apps: dict[str | int, Typer] = {}

    def __init__(self, allow_get: bool = True, allow_edit: bool = True) -> None:
        self.allow_get = allow_get
        self.allow_edit = allow_edit

    def __call__(
        self,
        id_: str | int | None = None,
        cb: Callable | None = None,
        cmds: list[Callable] | Callable | None = None,
        tprs: list[Typer] | Typer | None = None,
        allow_get: bool | None = None,
        allow_edit: bool | None = None,
    ) -> Typer:
        allow_get = self.allow_get if allow_get is None else allow_get
        allow_edit = self.allow_edit if allow_edit is None else allow_edit
        # create new app or try to get app
        app = None
        if id_ is None:
            id_ = _different(self.apps)
        elif allow_get:
            app = self.apps.get(id_, None)
        if app is None:
            app = Typer(name=str(id_))
        elif not self.allow_edit:
            return app
        if cb is not None:
            app.callback()(cb)
        if cmds is not None:
            app.registered_commands = []
            if isinstance(cmds, Callable):
                cmds = [cmds]
            for cmd in cmds:
                app.command()(cmd)
        if tprs is not None:
            app.registered_groups = []
            if isinstance(tprs, Typer):
                tprs = [tprs]
            for tpr in tprs:
                app.add_typer(tpr)
        self.apps[id_] = app
        return app


tf = Tf()
tfn = Tf(allow_get=False)


def _assert_result(result: Any, func: Callable, *args, **kwargs):
    resultcls = type(result)
    if issubclass(resultcls, Exception):
        with pytest.raises(resultcls, match=result.args[0]):
            func(*args, **kwargs)
    else:
        funcs = app_funcs_from_keys(*args, **kwargs)
        assert funcs == result


@pytest.fixture
def _nested() -> dict[str | None, Typer]:
    # a4
    app4 = Typer(name="a4")
    app4.callback()(ff(40))
    app4.command()(ff(41))
    app4.command()(ff(42))
    # a3
    app3 = Typer(name="a3")
    app3.callback()(ff(30))
    app3.command()(ff(31))
    app3.command()(ff(32))
    # a2
    app2 = Typer(name="a2")
    app2.callback()(ff(20))
    app2.command()(ff(21))
    app2.add_typer(app3)
    app2.add_typer(app4)
    # a1
    app1 = Typer(name="a1")
    app1.callback()(ff(10))
    app1.command()(ff(11))
    app1.add_typer(app2)
    # dict
    nest = {app.info.name: app for app in [app1, app2, app3, app4]}
    return nest


@pytest.fixture
def app_name() -> str:
    return "invalid"


@pytest.fixture
def app(_nested, app_name: str) -> Typer:
    return _nested[app_name]


class Test_app_funcs_from_keys:
    def test_single_command_with_callback(self) -> None:
        app2 = Typer()
        app2.callback()(ff(1))
        app2.command()(ff(2))
        funcs = app_funcs_from_keys(app2)
        assert funcs == [ff(1), ff(2)]

    def test_single_command_without_callback(self) -> None:
        app1 = Typer()
        app1.command()(ff(2))
        funcs = app_funcs_from_keys(app1)
        assert funcs == [ff(2)]

    def test_single_command_only_callback(self) -> None:
        app0 = Typer()
        app0.callback()(ff(1))
        with pytest.raises(ValueError, match="has no registered commands"):
            app_funcs_from_keys(app0)

    @pytest.mark.parametrize(
        ["app_name", "keys", "ret"],
        [
            ["a1", [], ValueError("groups but no cmdkey")],
            ["a1", ["a2"], ValueError("groups but no cmdkey")],
        ],
    )
    def test_single_command_no_cmdkey_1(self, _nested, app_name, keys, ret) -> None:
        _assert_result(ret, app_funcs_from_keys, _nested[app_name])

    VE1 = ValueError("has no registered commands")
    VE2 = ValueError("has multiple commands but no cmdkey was given")
    VE3 = ValueError("has groups but no cmdkey was given")
    tests = {
        "no-key-l1": [tf(cmds=ff(1)), [], [ff(1)]],
        "no-key-l1_no-cmds": [tf(), [], VE1],
        "no-key-l1_mult-cmds": [tf(cmds=[ff(), ff()]), [], VE2],
        "no-key-l1_groups": [tf(cmds=ff(), tprs=tf()), [], VE3],
        "no-key-l2": [tf(tprs=tf("a2", cmds=ff(1))), ["a2"], [ff(1)]],
        "no-key-l2_no-cmds": [tf(tprs=tf("a2", cmds=[])), ["a2"], VE1],
        "no-key-l2_mult-cmds": [tf(tprs=tf("a2", cmds=[ff(), ff()])), ["a2"], VE2],
        "no-key-l2_groups": [tf(tprs=tf("a2", cmds=ff(), tprs=tf())), ["a2"], VE3],
    }

    @pytest.mark.parametrize(["app", "keys", "ret"], tests.values(), ids=tests.keys())
    def test_single_command(self, app: Typer, keys: list[str], ret) -> None:
        _assert_result(ret, app_funcs_from_keys, app, keys=keys)

    def test_exceptions(self, _nested) -> None:
        # contains groups, but no keys
        with pytest.raises(VE, match="no single command (no keys were given)"):
            app_funcs_from_keys(_nested)
        # no valid command found matching
        with pytest.raises(VE, match="No valid command found matching"):
            app_funcs_from_keys(_nested, ["a2"])
        with pytest.raises(VE, match="No valid command found matching"):
            app_funcs_from_keys(_nested, ["a2", "a3"])
        # has no registered commands
        _nested.registered_groups = []
        _nested.registered_commands = []
        with pytest.raises(VE, match="has no registered commands"):
            app_funcs_from_keys(_nested)

    def test_match_single_command(self, _nested: Typer) -> None:
        app_funcs_from_keys(_nested, ["a2", "a3", "a4"])

    def test_normal(self, _nested: Typer) -> None:
        # lvl1 command
        assert app_funcs_from_keys(_nested, ["f11"]) == [ff(10), ff(11)]
        # lvl2 command
        funcs = app_funcs_from_keys(_nested, ["a2", "f21"])
        assert funcs == [ff(10), ff(20), ff(21)]
        # lvl3 command
        funcs = app_funcs_from_keys(_nested, ["a2", "a3", "f32"])
        assert funcs == [ff(10), ff(20), ff(30), ff(32)]


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
