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


def f(id_: int | str | None = None) -> Callable:
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


def maketyper(
    name: str | None = None,
    cb: Callable | None = None,
    cmds: list[Callable] | Callable | None = None,
    tprs: list[Typer] | Typer | None = None,
):
    app = Typer(name=name)
    if cb is not None:
        app.callback()(cb)
    if cmds is not None:
        if isinstance(cmds, Callable):
            cmds = [cmds]
        for cmd in cmds:
            app.command()(cmd)
    if tprs is not None:
        if isinstance(tprs, Typer):
            tprs = [tprs]
        for tpr in tprs:
            app.add_typer(tpr)
    return app


class Tf:
    """(Typer) App Factory"""

    apps: dict[str | int, Typer] = {}

    def __init__(self, force_new: bool = False) -> None:
        self.force_new = force_new

    def __call__(
        self,
        id_: str | int | None = None,
        force_new: bool | None = None,
        *args,
        **kwargs,
    ) -> Typer:
        if id_ is None:
            id_ = _different(self.apps)
        force_new = self.force_new if force_new is None else force_new
        if force_new:
            app = maketyper(*args, **kwargs, name=str(id_))
            self.apps[id_] = app
            return app
        return self.apps.setdefault(id_, maketyper(*args, **kwargs, name=str(id_)))


t = Tf()
tn = Tf(force_new=True)


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
    app4.callback()(f(40))
    app4.command()(f(41))
    app4.command()(f(42))
    # a3
    app3 = Typer(name="a3")
    app3.callback()(f(30))
    app3.command()(f(31))
    app3.command()(f(32))
    # a2
    app2 = Typer(name="a2")
    app2.callback()(f(20))
    app2.command()(f(21))
    app2.add_typer(app3)
    app2.add_typer(app4)
    # a1
    app1 = Typer(name="a1")
    app1.callback()(f(10))
    app1.command()(f(11))
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
    def test_single_command_only_callback(self) -> None:
        app0 = Typer()
        app0.callback()(f(1))
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

    t_0_1 = maketyper("t", cb=f("f0"), cmds=[f("f1")])
    VE1 = ValueError("has no registered commands")
    VE2 = ValueError("has multiple commands but no cmdkey was given")
    VE3 = ValueError("has groups but no cmdkey was given")
    tests1 = {
        "no-key": [t(cmds=f(1)), [], [f(1)]],
        "no-key_with-cb": [t(cb=f(1), cmds=f(2)), [], [f(1), f(2)]],
        "no-key_no-cmds": [t(), [], VE1],
        "no-key_mult-cmds": [t(cmds=[f(), f()]), [], VE2],
        "no-key_groups": [t(cmds=f(), tprs=t()), [], VE3],
        "app-key": [t(tprs=tn("t", cmds=f(1))), ["t"], [f(1)]],
        "app-key_with-cb": [t(tprs=t_0_1), ["t"], [f("f0"), f("f1")]],
        "app-key_no-cmds": [t(tprs=tn("t", cmds=[])), ["t"], VE1],
        "app-key_mult-cmds": [t(tprs=tn("t", cmds=[f(), f()])), ["t"], VE2],
        "app-key_groups": [t(tprs=tn("t", cmds=f(), tprs=t())), ["t"], VE3],
    }

    @pytest.mark.parametrize(["app", "keys", "ret"], tests1.values(), ids=tests1.keys())
    def test_no_key_or_app_key(self, app: Typer, keys: list[str], ret) -> None:
        _assert_result(ret, app_funcs_from_keys, app, keys=keys)

    t_0_12 = maketyper("t", cb=f("f0"), cmds=[f("f1"), f("f2")])
    VE4 = ValueError("has multiple commands matching")
    VE5 = ValueError("has no command matching")
    tests2 = {
        "l1": [t(cmds=f("f")), ["f"], [f("f")]],
        "l1_wth-cb": [t(cb=f(1), cmds=f("f")), ["f"], [f(1), f("f")]],
        "l1_mult-cmd-match": [t(cmds=[f("f"), f("f")]), ["f"], VE4],
        "l1_no-cmd-match": [t(cmds=f("f")), ["g"], VE5],
        "l2_with-cb": [t(tprs=t_0_12), ["t", "f1"], [f("f0"), f("f1")]],
        "l2": [t(tprs=tn("t", cmds=f("f"))), ["t", "f"], [f("f")]],
        "l2_mult-cmd-match": [t(tprs=tn("t", cmds=[f("f"), f("f")])), ["t", "f"], VE4],
        "l2_no-cmd-match": [t(tprs=tn("t", cmds=f("f"))), ["t", "g"], VE5],
    }

    @pytest.mark.parametrize(["app", "keys", "ret"], tests2.values(), ids=tests2.keys())
    def test_cmd_key(self, app: Typer, keys: list[str], ret) -> None:
        _assert_result(ret, app_funcs_from_keys, app, keys=keys)


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
        inner.__signature__ = Signature(list((mega_params | new_params).values()))  # type: ignore
        inner.__annotations__ = get_annotations(megafunc) | get_annotations(newfunc)
        return inner

    return outer


def f0(a=1, b=2, c=3):
    print(f"f: {a},{b},{c}")


def f1(b=4, c=5, d=6):
    print(f"f1: {b},{c},{d}")


def f2(e=7):
    print(f"f2: {e}")


into_f = assimilate(f0)


def test_assimilate():
    print()

    z1 = assimilate(f0)(f1)
    print(z1.__name__, inspect.signature(z1))
    z1(a=11, b=22, c=33, d=44)
    print()

    z2 = assimilate(z1)(f2)
    print(z2.__name__, inspect.signature(z2))
    z2(a=11, b=22, c=33, d=44, e=55)
    print()
