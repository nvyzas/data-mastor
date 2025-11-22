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
    edit_typer,
    funcs_to_run,
    make_typer,
    printctx,
)
from data_mastor.utils import mock_function_factory


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


y = app_with_yaml_support
EXC_MSG_NO_CMDS = "Could not get a command for this Typer instance"


t = partial(Tf(), invoke_without_command=False)
f = partial(mock_function_factory, func=printctx)
ai = partial(edit_typer, invoke_without_command=True, one_shot=True)

t00 = t("00")  # empty
t10 = t("10", cb=f("cb1"))
t01 = t("01", cmds=f("cmd1"))
Oc = Outcome
i = CliRunner().invoke


class Test_app_standard:
    tests = {
        "t00": [t00, RuntimeError(EXC_MSG_NO_CMDS)],
        "t10": [t10, Oc({"exit_code": 2})],
        "t10_auto-invoke": [ai(t10), Oc({"exit_code": 0})],
    }

    @pytest.mark.parametrize(["app", "outcome"], tests.values(), ids=tests.keys())
    def test_run_basic(self, app: Typer, outcome) -> None:
        assert_outcome(outcome, i, app)


class Test_app_with_yaml_support:
    def test_callback(self):
        app = y(t00)
        assert app.registered_callback is not None
        app = y(t10)
        assert app.registered_callback is not None

    tests = {
        "t00_no-yaml": [t00, False, RuntimeError(EXC_MSG_NO_CMDS)],
        "t00": [t00, True, Oc({"exit_code": 2})],
        "t00_auto-invoke": [ai(t00), True, Oc({"exit_code": 0})],
        "t10": [t10, True, Oc({"exit_code": 2})],
        "t10_auto-invoke": [ai(t10), True, Oc({"exit_code": 0})],
        "t10_no-yaml": [t10, False, Oc({"exit_code": 2})],
        "t10_auto-invoke_no-yaml": [ai(t10), False, Oc({"exit_code": 0})],
    }

    @pytest.mark.parametrize(
        ["app", "yaml", "outcome"], tests.values(), ids=tests.keys()
    )
    def test_run_basic(self, app: Typer, yaml: bool, outcome) -> None:
        app_ = app_with_yaml_support(app) if yaml else app
        func = CliRunner().invoke
        assert_outcome(outcome, func, app_)
