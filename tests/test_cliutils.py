from collections.abc import Callable
from functools import partial
from typing import Any

import pytest
from click.testing import Result
from rich import print
from typer import Context, Typer
from typer.testing import CliRunner

from data_mastor.cliutils import (
    Tf,
    app_with_yaml_support,
    funcs_to_run,
    make_typer,
    printctx,
)
from data_mastor.utils import mock_function_factory

t = Tf()
tn = Tf(force_new=True)
f = mock_function_factory


def assert_outcome(outcome: Any, func: Callable, *args, **kwargs):
    outcomecls = type(outcome)
    if issubclass(outcomecls, Exception):
        with pytest.raises(outcomecls, match=outcome.args[0]):
            func(*args, **kwargs)
    else:
        return_value = func(*args, **kwargs)
        assert return_value == outcome


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
            funcs_to_run(app0)

    @pytest.mark.parametrize(
        ["app_name", "keys", "ret"],
        [
            ["a1", [], ValueError("groups but no cmdkey")],
            ["a1", ["a2"], ValueError("groups but no cmdkey")],
        ],
    )
    def test_single_command_no_cmdkey_1(self, _nested, app_name, keys, ret) -> None:
        assert_outcome(ret, funcs_to_run, _nested[app_name])

    t_0_1 = make_typer("t", cb=f("f0"), cmds=[f("f1")])
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
        assert_outcome(ret, funcs_to_run, app, keys=keys)

    t_0_12 = make_typer("t", cb=f("f0"), cmds=[f("f1"), f("f2")])
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
        assert_outcome(ret, funcs_to_run, app, keys=keys)


def _check(result: Result):
    print(result.output)
    assert result.exit_code == 0


y = app_with_yaml_support
i = CliRunner().invoke
EXC_MSG_NO_CMDS = "Could not get a command for this Typer instance"


class Test_app_with_yaml_support:
    t = partial(t, invoke_without_command=True)
    f = partial(f, func=printctx)
    t("t00")  # empty
    t("t10", cb=f("cb1"))
    t("t01", cmds=f("cmd1"))

    def test_callback(self):
        app = y(self.t("t00"))
        assert app.registered_callback is not None
        app = y(self.t("t10"))
        assert app.registered_callback is not None

    # def test_simple1(self):
    #     r = CliRunner().invoke(, "--help")
    #     _check(r)

    def test_invoke(self):
        r = i(y(t("t00")), "--help")
        _check(r)

    def test_run_basic(self) -> None:
        # empty typer app - autoinvoke, no yaml support
        with pytest.raises(RuntimeError, match=EXC_MSG_NO_CMDS):
            i(t("t00"))
        # empty typer app - autoinvoke)
        r = i(y(t("t00")))
        assert r.exit_code == 0
        # empty typer app - no autoinvoke)
        r = i(y(t("t00_noinvoke", invoke_without_command=False)))
        assert r.exit_code == 2
        # single callback - autoinvoke, no yaml support
        r = i((t("t10")))
        assert r.exit_code == 0
        # single callback - autoinvoke, no yaml support
        r = i((t("t10")))
        assert r.exit_code == 0

        # callback without autoinvoke
        # i(
        # _check(r)
        # r = CliRunner().invoke((t("t00_noinvoke", invoke_without_command=False)))

    def test_run_help(self) -> None:
        r = CliRunner().invoke((t("tpr1")))
        _check(r)

    def test_run_noargs(self) -> None:
        r = CliRunner().invoke((t("tpr1")))
        _check(r)
