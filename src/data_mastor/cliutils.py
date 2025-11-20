import argparse
import os
from collections.abc import Callable, Sequence
from functools import partial, wraps
from inspect import Parameter, Signature, signature
from pathlib import Path
from typing import Annotated, Any, cast

import yaml
from click import get_current_context
from click.core import ParameterSource
from rich import print
from typer import Context, Option, Typer
from typer.models import TyperInfo

from data_mastor.utils import (
    _different,
    combine_funcs,
    mock_function_factory,
    replace_function_signature,
)


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


# DO replace all uses of this with nested_yaml_dict_get
def get_yamldict_key(
    yamlfile: str | Path, key: str, doraise: bool = False, default: Any = None
):
    if default is None:
        default = {}
    yamlfile = Path(yamlfile)
    if yamlfile.is_file():
        with open(yamlfile) as file:
            yamldict = yaml.safe_load(file)
            if key not in yamldict:
                msg = f"Top-level key '{key}' was not found"
                if doraise:
                    raise KeyError(msg)
                print(msg)
                return default
            return yamldict[key]
    else:
        msg = f"File '{yamlfile}' was not found"
        if doraise:
            raise FileNotFoundError(msg)
        print(msg)
        return default


def yaml_nested_dict_get(
    yamlpath: str | Path,
    keys: list[str] | str | None = None,
    trace_unknown_keys: bool = True,
    doraise: bool = True,
    debug: bool = False,
) -> tuple[list[str], Any]:
    exc: Exception

    yamlpath = Path(yamlpath)
    if not yamlpath.exists():
        exc = FileNotFoundError(f"File '{yamlpath}' does not exist")
        if doraise:
            raise exc
        elif debug:
            print(exc)
            return [], {}
        else:
            return [], {}

    with open(yamlpath) as file:
        yamlpart = yaml.safe_load(file)
    if keys is None:
        return [], yamlpart
    if isinstance(keys, str):
        keys = [keys]
    for i, key in enumerate(keys):
        if not isinstance(yamlpart, dict):
            exc = TypeError(f"Yaml element at '{keys[:i]}' is not a dictionary")
            if doraise:
                raise exc
            elif debug:
                print(exc)
                return keys[:i], yamlpart
            else:
                return keys[:i], yamlpart
        if key not in yamlpart.keys():
            exc = KeyError(f"Key '{key}' was not found in yaml dict at '{keys[:i]}'")
            if doraise:
                raise exc
            elif debug:
                print(exc)
                return keys[:i], yamlpart
            else:
                return keys[:i], yamlpart
        yamlpart = yamlpart[key]
    if trace_unknown_keys:
        unknown_keys = []
        while True:
            if not isinstance(yamlpart, dict):
                break
            marked_keys = [k for k in yamlpart if "!" in k]
            if len(marked_keys) > 1:
                raise KeyError(f"There are multiple marked keys: {marked_keys}")
            if len(marked_keys) == 0:
                break
            unknown_keys.append(marked_keys[0])
            yamlpart = yamlpart[unknown_keys[-1]]
        keys += unknown_keys
    return keys, yamlpart


def app_funcs_from_keys(app: Typer, keys: list[str] | str | None = None):
    if keys is None:
        keys = []
    elif isinstance(keys, str):
        keys = [keys]
    cmdkey = None
    funcs: list[Callable] = []
    tpr = app
    i = 0
    while True:
        # look for callback
        if tpr.registered_callback:
            if tpr.registered_callback.callback is None:
                print(f"WARNING: Skipping NULL callback from App({i})")
            else:
                _f = tpr.registered_callback.callback
                print(f"Adding callback '{_f}' from App({i})")
                funcs.append(_f)
        # break if we reached the last key
        if i >= len(keys):
            break
        # go to the next group
        key = keys[i]
        key_tprs = []
        for g in tpr.registered_groups:
            if g.typer_instance is None:
                print(f"WARNING: Skipping NULL group ({g}) from App({i}")
                continue
            if g.name == key or g.typer_instance.info.name == key:
                key_tprs.append(g.typer_instance)
        if len(key_tprs) > 1:
            raise ValueError(f"App({i}) has multiple subapps named '{key}'")
        if not key_tprs:
            if i == len(keys) - 1:
                # the last key is not a group name (it must be a command name then)
                cmdkey = key
                break
            raise ValueError(f"App({i}) has no subapps named '{key}')")
        tpr = key_tprs[0]
        i += 1

    # look for commands

    # check if the current app (tpr variable) has at least one command
    if not tpr.registered_commands:
        raise ValueError(f"App({i}) has no registered commands")

    # if the last key corresponds to a command, use it to find the command
    if cmdkey is not None:
        cmds = [_ for _ in tpr.registered_commands if _.callback is not None]
        key_cmds = [_ for _ in cmds if key in [_.name, _.callback.__name__]]  # type: ignore
        if len(key_cmds) > 1:
            raise ValueError(f"App{i} has multiple commands matching '{cmdkey}'")
        if not key_cmds:
            raise ValueError(f"App{i} has no command matching '{cmdkey}')")
        _f = key_cmds[0].callback
        print(f"Adding command '{_f}' from App({i})")
        funcs.append(_f)  # type: ignore
        return funcs

    # the last key corresponds to an app, look for a single command
    if len(tpr.registered_commands) > 1:
        raise ValueError(f"App({i}) has multiple commands but no cmdkey was given")
    if tpr.registered_groups:
        raise ValueError(f"App({i}) has groups but no cmdkey was given")
    if tpr.registered_commands[0].callback is None:
        raise ValueError(f"App({i}) has a single command but it is NULL")
    _f = tpr.registered_commands[0].callback
    print(f"Adding (single) command '{_f}' from App({i})")
    funcs.append(_f)
    return funcs


def update_kwargs_from_context(
    kwargs: dict[str, Any],
    ctx: Context,
    edit_ctx_param_values=False,
    edit_ctx_param_sources=True,
) -> dict[str, Any]:
    updated_kwargs = {}
    for k, v in kwargs.items():
        if k not in ctx.params:
            print(f"Using arg {k}={v} (unspecified)")
            updated_kwargs[k] = v
            continue
        val = ctx.params[k]
        src = ctx.get_parameter_source(k)
        if src == ParameterSource.COMMANDLINE:
            print(f"Ignoring arg {k}={v} (overriden by value from cmdline: {val})")
            continue
        if v != val:
            print(f"Using arg {k}={v} (instead of value from {src}: {val})")
        else:
            print(f"Using arg {k}={v} (same as ctx value from {src})")
        if edit_ctx_param_sources:
            # treat the arg as if it came from the cmdline
            ctx.set_parameter_source(k, ParameterSource.COMMANDLINE)
        updated_kwargs[k] = v

    if edit_ctx_param_values:
        # update value in context
        for k, v in updated_kwargs.items():
            ctx.params[k] = v
    return updated_kwargs


ARGS_YAMLPATH = "args.yml"


def app_with_yaml_support(
    app: Typer,
    yamlpath: Path = Path(ARGS_YAMLPATH),
    yamlkeys: list[str] | None = None,
) -> Typer:
    # use yaml only if no args were provided in the cmdline
    parser = argparse.ArgumentParser(add_help=False, exit_on_error=False)
    parser.add_argument("--yaml", action="store_true", default=False)
    parser.add_argument("--yamlpath", type=str, default=ARGS_YAMLPATH)
    # FIX skip mistype suggestions (added in Python 3.14)
    args, unknown = parser.parse_known_args()
    # if not args.yaml:
    #     return app

    keys = yamlkeys
    if yamlkeys is None:
        if app.info.name:
            print(f"Using app name ({app.info.name}) as top-level yaml key")
            keys = [app.info.name]
        else:
            # TODO use module name as a fallback
            raise ValueError("App has no name to infer yaml key from")

    # get yamlargs and corresponding function keys
    all_keys, yamlargs = yaml_nested_dict_get(yamlpath, keys=keys)
    print(f"Args from {yamlpath.absolute()} under {all_keys}:")
    print(yamlargs)
    if not isinstance(yamlargs, dict):
        print("WARNING: args from yaml is not a dict. Assuming an empty dict")
        yamlargs = {}

    # get combined command
    cmd_names = list(map(lambda s: s.replace("!", ""), all_keys[1:]))
    funcs = app_funcs_from_keys(app, cmd_names)

    # def up_yamlargs(ctx: Context):
    #     preparser_args = [_ for _ in ctx.args if _ in ["--yaml", "--yamlpath"]]
    #     if preparser_args:
    #         print(f"Preparser args: {ctx.args}")
    #     return update_kwargs_from_context(yamlargs, ctx)

    # combined = combine_funcs(funcs, kwargs_updater=up_yamlargs)

    def _find_callback(
        callback: str, tpr: Typer, grp: TyperInfo | None = None
    ) -> Callable[..., Any] | None:
        if tpr.registered_callback and (cb := tpr.registered_callback.callback):
            if (
                (grp and grp.name == callback)
                or (tpr.info.name == callback)
                or (cb.__name__ == callback)
            ):
                return cb
        return None
        raise ValueError(f"Could not find callback {callback} in typer {tpr}")

    def _find_command_callback(callback: str, tpr: Typer) -> Callable[..., Any] | None:
        for cmd in tpr.registered_commands:
            if (cb := cmd.callback) is None:
                continue
            if cmd.name == callback or cb.__name__ == callback:
                return cb
        return None
        raise ValueError(f"Could not find command callback {callback} in typer {tpr}")

    def with_updated_kwargs(func):
        @wraps(func)
        def wrapper(ctx: Context, **kwargs):
            if ctx.invoked_subcommand and ctx.invoked_subcommand not in cmd_names:
                raise RuntimeError(f"{ctx.invoked_subcommand} is not in {all_keys}")
            print(f"Meta kwargs: {ctx.meta['updated_kwargs']}")
            kwargs.update(ctx.meta["updated_kwargs"])
            kwargs["ctx"] = ctx
            kw = {k: v for k, v in kwargs.items() if k in signature(func).parameters}
            print(f"Running wrapped {func} with {kw}")
            func(**kw)

        replace_function_signature(wrapper, [wrapper, func], no_variadic=True)
        return wrapper

    tpr = app
    print(funcs)
    for i, f in enumerate(funcs):
        funcs[i] = with_updated_kwargs(f)

    print(funcs)
    app.registered_callback.callback = funcs[0]
    app.registered_commands[0].callback = funcs[1]

    def update_kwargs(ctx: Context, **kwargs):
        #     preparser_args = [_ for _ in ctx.args if _ in ["--yaml", "--yamlpath"]]
        #     if preparser_args:
        #         print(f"Preparser args: {ctx.args}")
        ctx.meta["updated_kwargs"] = update_kwargs_from_context(yamlargs, ctx)

    # to help update_kwargs_from_context work properly, ctx.args must include all args
    # for this, we use a combined signature from across the whole call chain
    replace_function_signature(update_kwargs, [update_kwargs] + funcs, no_variadic=True)

    # root callback
    funcs = [update_kwargs]
    if app.registered_callback and app.registered_callback.callback:
        funcs.append(app.registered_callback.callback)
    root_callback = combine_funcs(funcs)
    app.callback(invoke_without_command=True)(root_callback)
    # newapp = Typer(name=megafunc.__name__, add_completion=False)
    # newapp.command()(megafunc)
    return app


Opt = Option


# REF (copilot) use this to refactor spiders
def opt(
    dtype: type, names: str | list[str], help: str | None, panel: str | None, **kwargs
) -> Annotated:
    if isinstance(names, str):
        names = [names]
    return Annotated[dtype, Option(*names, help=help, rich_help_panel=panel, **kwargs)]


def _print_ctx(ctx: Context):
    info = {
        "id": id(ctx),
        "params": ctx.params,
        "invoked": ctx.invoked_subcommand,
        "command": ctx.command,
        "command_path": ctx.command_path,
    }
    print(info)


f = mock_function_factory
# app
app = Typer(name="cliutils")
app.callback()(f("root_cb", func=_print_ctx))


@app.callback()
def testcb(ctx: Context, a=1):
    _print_ctx(ctx)


@app.command()
def test(ctx: Context, a=2, b=1):
    _print_ctx(ctx)


# # subapp
# subapp = Typer(name="subapp")


# @subapp.command()
# def subtest(ctx: Context, a=2, c=7):
#     print(f"Running subtest cmd with: {ctx}")
#     print(ctx.parent)
#     print(ctx.params)
#     print(f"Invoked: {ctx.invoked_subcommand}")
#     print(ctx.command)
#     print(ctx.command_path)
#     print()


# @subapp.command()
# def subtest2(ctx: Context, a=3, c=8):
#     print(f"Running subtest2 cmd with: {ctx}")
#     print(ctx.parent)
#     print(ctx.params)
#     print(f"Invoked: {ctx.invoked_subcommand}")
#     print(ctx.command)
#     print(ctx.command_path)
#     print()


# app.add_typer(subapp)


# # zubapp
# zubapp = Typer(name="zubapp")


# @zubapp.command()
# def zubtest(ctx: Context, a=2, c=7):
#     print(f"Running zubtest cmd with: {ctx}")
#     print(ctx.parent)
#     print(ctx.params)
#     print(f"Invoked: {ctx.invoked_subcommand}")
#     print(ctx.command)
#     print(ctx.command_path)
#     print()


# @zubapp.command()
# def zubtest2(ctx: Context, a=3, c=8):
#     print(f"Running zubtest2 cmd with: {ctx}")
#     print(ctx.parent)
#     print(ctx.params)
#     print(f"Invoked: {ctx.invoked_subcommand}")
#     print(ctx.command)
#     print(ctx.command_path)
#     print()


# subapp.add_typer(zubapp)

if __name__ == "__main__":
    app_with_yaml_support(app)()
    # app()
