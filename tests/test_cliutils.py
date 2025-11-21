from collections.abc import Callable
from functools import partial
from typing import Any, Self

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


class Outcome:
    def __init__(
        self,
        retprops: dict[str, Any] | None = None,
        objprops: list[tuple[Any, dict[str, Any]]] | None = None,
        retvalue=None,
    ) -> None:
        self.retvalue = retvalue
        self.retprops = retprops
        self.objprops = objprops

    def _check(self, ret: Any):
        if self.retprops is not None:
            for k, v in self.retprops.items():
                assert getattr(ret, k) == v
        if self.objprops is not None:
            for obj, props in self.objprops:
                for k, v in props.items():
                    assert getattr(obj, k) == v
        if self.retvalue is not None:
            assert ret == self.retvalue


def assert_outcome(outcome: Outcome | Exception | Any, func: Callable, *args, **kwargs):
    if isinstance(outcome, Exception):
        with pytest.raises(type(outcome), match=outcome.args[0]):
            func(*args, **kwargs)
    elif isinstance(outcome, Outcome):
        outcome._check(func(*args, **kwargs))
    else:
        Outcome(retvalue=outcome)._check(func(*args, **kwargs))


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


def _check(result: Result):
    print(result.output)
    assert result.exit_code == 0


y = app_with_yaml_support
i = CliRunner().invoke
EXC_MSG_NO_CMDS = "Could not get a command for this Typer instance"


class Test_app_with_yaml_support:
    t = partial(t, invoke_without_command=False)
    ti = partial(t, invoke_without_command=True)
    f = partial(f, func=printctx)
    t("t00")  # empty
    t("t10", cb=f("cb1"))
    t("t01", cmds=f("cmd1"))
    Oc = Outcome

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

    tests = {
        "t00_no-yaml": [t("t00"), False, RuntimeError(EXC_MSG_NO_CMDS)],
        "t00": [t("t00", invoke_without_command=False), True, Oc({"exit_code": 2})],
        "t00_auto-invoke": [ti("t00"), True, Oc({"exit_code": 0})],
    }

    @pytest.mark.parametrize(
        ["app", "yaml", "outcome"], tests.values(), ids=tests.keys()
    )
    def test_run_basic(self, app: Typer, yaml: bool, outcome) -> None:
        app_ = app_with_yaml_support(app) if yaml else app
        func = CliRunner().invoke
        assert_outcome(outcome, func, app_)
        # empty typer app - autoinvoke, no yaml support
        # with pytest.raises(RuntimeError, match=EXC_MSG_NO_CMDS):
        #     i(t("t00"))

        # empty typer app - autoinvoke)
        # r = i(y(t("t00")))
        # assert r.exit_code == 0
        # # empty typer app - no autoinvoke)
        # r = i(y(t("t00_noinvoke", invoke_without_command=False)))
        # assert r.exit_code == 2
        # # single callback - autoinvoke, no yaml support
        # r = i((t("t10")))
        # assert r.exit_code == 0
        # # single callback - autoinvoke, no yaml support
        # r = i((t("t10")))
        # assert r.exit_code == 0

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
