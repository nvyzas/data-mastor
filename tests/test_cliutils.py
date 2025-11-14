import inspect
from functools import wraps
from inspect import Signature, get_annotations, signature

import click
import pytest
import typer
from click.testing import Result
from rich import print
from typer.testing import CliRunner

calls = []


def add(name):
    calls.append(name)


def cb0(ctx: typer.Context):
    print("Callback 0")
    add(0)


def cb1(ctx: typer.Context):
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


def test_multi_callbacks():
    print()
    check([])
    # app0
    app0 = typer.Typer(name="root")
    app0.callback()(cb0)
    result = CliRunner().invoke(app0)
    assert result.exception.code == 2  # type: ignore
    check([])
    # app0 auto-invoke
    app0.info.invoke_without_command = True
    result = CliRunner().invoke(app0)
    check([0])
    # app1 auto-invoke
    app0.info.invoke_without_command = True
    app1 = typer.Typer(name="one", invoke_without_command=True)
    app1.callback()(cb1)
    app0.add_typer(app1)
    result = CliRunner().invoke(app0)
    _printresult(result)
    check([0, 1])


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
