from functools import partial as p
from typing import Any
from unittest.mock import MagicMock

import pytest
from click.testing import Result
from pytest_mock import MockerFixture
from typer import Context, Typer
from typer.testing import CliRunner

from data_mastor.cliutils import (
    Tf,
    app_with_yaml_support,
    edit_typer,
    printctx,
    read_yaml,
)
from data_mastor.utils import (
    Assertion,
    FunctionFactory,
    Outcome,
    assert_result_exit_code,
    patch_path,
    sigpart,
)

EXC_MSG_NO_CMDS = "Could not get a command for this Typer instance"


# typers
def dummy_sig(ctx: Context, a: int = 0, b: int = 0, c: int = 0, d: int = 0):
    pass


# mock_read_yaml = patch(
#     patch_path(read_yaml, at=app_with_yaml_support), new=MagicMock
# ).start()
t = p(Tf(), invoke_without_command=False, add_completion=False)
ff = FunctionFactory()
f = p(ff.__call__, func=printctx)
s = p(sigpart, dummy_sig, "ctx")
f10 = f("cb10", sig=s("a", "b", "c", "d"))
f11 = f("cmd11", sig=s("a", "b", "c", "d"))


# test helpers
def edit_name(app: Typer, prefix: str = "", suffix: str = ""):
    app.info.name = prefix + (app.info.name or "") + suffix


c = p(edit_typer)
ai = p(
    edit_typer,
    invoke_without_command=True,
    one_shot=True,
    funcs=[p(edit_name, suffix="ai")],
)


class Test_app_with_yaml_support:
    @pytest.fixture(scope="class")
    def mock_read_yaml(self, class_mocker: MockerFixture) -> MagicMock:
        print("fixture mock_read_yaml")
        mock = class_mocker.patch(patch_path(read_yaml, at=app_with_yaml_support))
        return mock

    # @pytest.fixture(autouse=True)
    # def reset_mock_read_yaml(
    #     self, mock_read_yaml: MagicMock, id_: int
    # ) -> Generator[Mock, None, None]:
    #     print(f"{id_} before, {mock_read_yaml}")
    #     belo[id_] = "done"
    #     mock_read_yaml.reset_mock()
    #     yield mock_read_yaml
    #     print(f"{id_} after, {mock_read_yaml}")

    @pytest.fixture(scope="class")
    def setup_infrastructure(self, mock_read_yaml):
        print("fixture setup_infrastructure")
        t(0)  # empty
        t(10, cb=f10)
        t(1, cmds=f11)
        t(11, cb=f10, cmds=f11)

    @pytest.fixture
    def app(self, request, setup_infrastructure, id_):
        print(f"Setting up app for id {id_}")
        param = request.param[-1]
        for pnext in request.param[-2::-1]:
            param = pnext(param)

        # megafunc = request.param[0]
        # for func in request.param[1:]:
        #     megafunc = p(megafunc, func)

        # megafunc = request.param[-1]
        # for func in request.param[-2:-1]:
        #     megafunc = p(func, megafunc)
        return param

    @pytest.fixture
    def reset_mocks(self, id_, mock_read_yaml):
        yield
        print(f"Resetting mocks from id {id_}")
        mock_read_yaml.reset_mock()
        for m in ff.mf._mocks.values():
            m.reset_mock()

    @pytest.fixture
    def assertions(self, expectations) -> list[Assertion]:
        if not isinstance(expectations, list):
            expectations = [expectations]
        return [_ if isinstance(_, Assertion) else Assertion(_) for _ in expectations]

    @staticmethod
    def idfunc(val) -> str:
        # testcase id
        if isinstance(val, int):
            return str(val)
        # app name
        if isinstance(val, Typer):
            return "t" + (val.info.name or "")
        # yaml args
        elif isinstance(val, dict) or val is None:
            if val is None:
                return "plain"
            else:
                argstr = "_".join(map(lambda t: str(t[0]) + str(t[1]), val.items()))
                return "[" + argstr + "]"
        # cli args
        elif isinstance(val, str):
            argstr = val.replace("--", "").replace("=", "")
            return "[" + argstr + "]"
        return ""

    class Plain:
        def __init__(self, value) -> None:
            self.value = value

    @pytest.mark.parametrize(
        ["id_", "app", "args", "yamlargs", "expectations"],
        [
            # t00
            [1, [t, 0], "", Plain(1), RuntimeError(EXC_MSG_NO_CMDS)],
            [2, [t, 0], "", {}, {"exit_code": 2}],
            [3, [ai, t, 0], "", Plain(2), RuntimeError(EXC_MSG_NO_CMDS)],
            [4, [ai, t, 0], "", {}, assert_result_exit_code],
            # # t01
            [5, [t, 1], "", Plain(3), lambda _: _.exit_code == 0],
            [6, [t, 1], "", {}, {"exit_code": 0}],
            [7, [ai, t, 1], "", Plain(4), {"exit_code": 0}],
            [8, [ai, t, 1], "", {}, {"exit_code": 0}],
            [9, [t, 1], "--a=1 --b=1", Plain(5), {"exit_code": 0}],
            [10, [t, 1], "", {}, {"exit_code": 0}],
            # t10
            [11, [t, 10], "", Plain(6), {"exit_code": 2}],
            [12, [t, 10], "", {}, {"exit_code": 2}],
            [13, [ai, t, 10], "", Plain(7), {"exit_code": 0}],
            [14, [ai, t, 10], "", {}, {"exit_code": 0}],
            # # t11
            [15, [t, 11], "", Plain(8), {"exit_code": 2}],
            [16, [t, 11], "", {}, {"exit_code": 2}],
            [17, [ai, t, 11], "", Plain(9), {"exit_code": 0}],
            [18, [ai, t, 11], "", {}, {"exit_code": 0}],
        ],
        ids=idfunc,
        indirect=["app"],
    )
    def test_run_basic(
        self,
        id_: int,  # to easily find the testcase being debugged
        app: Typer,
        yamlargs: None | dict[str, Any],
        args: str,
        expectations: list[Any] | Any,  # converted to assertions by fixture
        assertions: list[Assertion],
        reset_mocks,
        mock_read_yaml,
    ) -> None:
        app = app if isinstance(yamlargs, self.Plain) else app_with_yaml_support(app)
        o = Outcome(CliRunner().invoke, app, args=args or None)().outcome
        assertions[0](o)
        for a in assertions[1:]:
            a(o)
        if isinstance(yamlargs, self.Plain) or (
            isinstance(o, Result) and o.exit_code != 0
        ):
            mock_read_yaml.assert_not_called()
        else:
            mock_read_yaml.assert_called_once()


class Test_assertion:
    def test_assertion_init(self) -> None:
        cat = Assertion.Category
        # default values
        assert Assertion(1) == Assertion(1, None) == Assertion(1, None, None)
        # tuple parsing
        assert Assertion(1) == Assertion((1, None)) == Assertion((1, None, None))
        # categories
        exc = Exception("hi")
        category_examples = {
            cat.EXCEPTION: exc,
            cat.TYPE: list,
            cat.TRUTHFUNC: lambda: True,
            cat.ATTRIBUTES: {"key": "value"},
            cat.EQUALS: 1,
        }
        for k, v in category_examples.items():
            assert Assertion(v) == Assertion(v, category=k)

    def test_assertfunc_equals(self) -> None:
        a = Assertion(2)
        a(2)
        with pytest.raises(AssertionError):
            a(5)
        b = Assertion(["a", "b"])
        b(["a", "b"])
        b.kwargs = {"identity_check": True}
        with pytest.raises(AssertionError):
            b(["a", "b"])

    def test_assertfunc_type(self):
        class Custom:
            pass

        class CustomSubcls(Custom):
            pass

        a = Assertion(Custom)
        a(Custom)
        a(CustomSubcls)
        a(Custom())
        a(CustomSubcls())

    def test_assertfunc_exceptions_equal(self):
        a = Assertion(Exception("hio"))
        # same type and msg
        a(Exception("hio"))
        # different type
        with pytest.raises(AssertionError):
            a(ValueError("hio"))
        a.kwargs = {"exact_type": False}
        a(ValueError("hio"))
        # different msg
        a(Exception("hi"))
        a(Exception("hio sir"))
        a.kwargs = {"exact_message": True}
        with pytest.raises(AssertionError):
            a(Exception("hi"))
        with pytest.raises(AssertionError):
            a(Exception("hio sir"))

    def test_assertfunc_callable(self) -> None:
        # function that includes assert

        def passing_assert():
            assert True

        Assertion(passing_assert)()

        def failing_assert():
            assert False

        with pytest.raises(AssertionError):
            Assertion(failing_assert)()

        # function with many args
        def f(a, b, c=1):
            assert a + b + c == 5

        a = Assertion(f)
        a(2, 2)
        a(1, 3)
        a(3, 1)
        a(1, 1, 3)
        a(4, 1, c=0)
        with pytest.raises(AssertionError):
            a(1, 1, 1)

        # check true_only arg using a lambda
        Assertion(lambda: True, kwargs={"true_only": True})()
        with pytest.raises(AssertionError):
            Assertion(lambda: False, kwargs={"true_only": True})()
