import inspect
from functools import wraps
from inspect import Signature, get_annotations, signature

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
    app0.callback()(cb0)
    return app0


@pytest.fixture
def app1() -> Typer:
    app1 = Typer(name="one", invoke_without_command=True)
    app1.callback()(cb1)
    return app1


@pytest.fixture
def app01(app0: Typer, app1: Typer):
    app0.add_typer(app1)
    return app0


def test_auto_invoke(app0, app01):
    print()
    check([])
    # app0 auto-invoke
    result = CliRunner().invoke(app0)
    check([0])
    # app1 auto-invoke
    result = CliRunner().invoke(app01, "one")
    _printresult(result)
    check([0, 1])


def cmds_from_keys(app: Typer, keys: list[str]):
    cmds = []
    candidates = [app]
    for key in keys:
        for cand in candidates:
            if key == cand.info.name:
                print(f"Found app for key '{key}'")
                break
        else:
            raise ValueError(f"Could not find app named {key}")
        if cand.registered_callback:
            print(f"Added callback for key '{key}'")
            cmds.append(cand.registered_callback.callback)
        instances = [_.typer_instance for _ in cand.registered_groups]
        candidates = [_ for _ in instances if _ is not None]
    return cmds


def test_cmd_from_keys_flat(app0):
    cmds = cmds_from_keys(app0, ["zero"])
    assert cmds == [cb0]
    with pytest.raises(ValueError):
        cmds = cmds_from_keys(app0, ["wrong"])
    with pytest.raises(ValueError):
        cmds = cmds_from_keys(app0, ["zero", "wrong"])
    # add commands
    app0.command("cmd0")(cmd0)
    cmds = cmds_from_keys(app0, ["cmd0"])
    assert cmds == [cmd0]


def test_cmd_from_keys_nested(app01):
    cmds = cmds_from_keys(app01, ["zero"])
    assert cmds == [cb0]
    cmds = cmds_from_keys(app01, ["zero", "one"])
    assert cmds == [cb0, cb1]
    with pytest.raises(ValueError):
        cmds = cmds_from_keys(app01, ["wrong"])
    with pytest.raises(ValueError):
        cmds = cmds_from_keys(app01, ["zero", "wrong"])
    with pytest.raises(ValueError):
        cmds = cmds_from_keys(app01, ["zero", "one", "wrong"])


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
