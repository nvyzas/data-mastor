import inspect
import random
from collections.abc import Callable
from functools import wraps
from inspect import Signature, get_annotations, signature
from typing import Any
from unittest.mock import MagicMock, Mock

import click
import pytest
from click.testing import Result
from rich import print
from typer import Context, Typer
from typer.testing import CliRunner

from data_mastor.cliutils import app_funcs_from_keys, app_with_yaml_support
from data_mastor.utils import _different

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


def _check(result: Result):
    print(result.output)
    assert result.exit_code == 0


yamlapp = app_with_yaml_support


def _invoke(ctx: Context, cmd: Callable):
    ctx.invoke(cmd)


def invoke(ctx: Context):
    pass


t(id_="lvl1", cmds=f("cmd1"))
t(id_="lvl1i", cmds=f("cmd1i", func=_invoke, sigfunc=invoke))


class Test_yaml_app:
    def test_yamlapp(self):
        app = yamlapp(t("lvl1"))
        assert app.registered_callback is not None

    # def test_simple1(self):
    #     r = CliRunner().invoke(, "--help")
    #     _check(r)

    def test_invoke(self):
        r = CliRunner().invoke(t("lvl1i"), "--help")
        _check(r)
