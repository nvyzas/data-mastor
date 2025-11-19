import argparse
import os
from collections.abc import Callable, Sequence
from functools import partial
from inspect import Parameter, Signature, signature
from pathlib import Path
from typing import Annotated, Any, cast

import yaml
from click.core import ParameterSource
from rich import print
from typer import Context, Option, Typer


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
                # the last key is not a group key (it should be a command key)
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


# NEXT replace with combine_funcs
def edit_function_signature(
    func,
    other_functions: list[Callable],
    no_variadic=False,
    edit_annotations=True,
    edit_name=True,
):
    # get parameters
    params: dict[str, Parameter] = {}
    for f in other_functions:
        sig = signature(cast(Callable[..., Any], f))
        params.update(sig.parameters)

    # remove variadic
    if no_variadic:
        to_del = [
            k for k, v in params.items() if v.kind in [v.VAR_POSITIONAL, v.VAR_KEYWORD]
        ]
        for k in to_del:
            del params[k]
        if to_del:
            print(f"Removed variadic signature elements: {to_del}")

    # sort parameters by kind
    order = {
        Parameter.POSITIONAL_ONLY: 0,
        Parameter.POSITIONAL_OR_KEYWORD: 1,
        Parameter.VAR_POSITIONAL: 2,
        Parameter.KEYWORD_ONLY: 3,
        Parameter.VAR_KEYWORD: 4,
    }
    sorted_params = dict(sorted(params.items(), key=lambda item: order[item[1].kind]))

    # edit signature
    func.__signature__ = Signature(list(sorted_params.values()))  # type: ignore

    # edit annotations
    if edit_annotations:
        annots = {name: param.annotation for name, param in sorted_params.items()}
        func.__annotations__ = annots

    # edit name
    if edit_name:
        func.__name__ = "__".join([_.__name__ for _ in other_functions])
    return func


def combine_funcs(
    funcs: Sequence[Callable], kwargs_updater: Callable | None = None
) -> Callable[..., None]:
    if not funcs:
        raise ValueError(f"Sequence of functions argument seems empty ({funcs})")

    def combined(**kwargs):
        print("\nCombined: start")
        if kwargs_updater is not None:
            params = signature(kwargs_updater).parameters
            kw = {k: v for k, v in kwargs.items() if k in params}
            print(f"Combined: calling '{kwargs_updater.__name__}' with: {kw}")
            updates = kwargs_updater(**kw)
            kwargs.update(updates)
        for f in funcs:
            params = signature(f).parameters
            kw = {k: v for k, v in kwargs.items() if k in params}
            print(f"Combined: calling '{f.__name__}' with: {kw}")
            f(**kw)
        print("Combined: end\n")

    updater = [kwargs_updater] if kwargs_updater else []
    edit_function_signature(combined, [*updater, *funcs], no_variadic=True)
    return combined


def update_kwargs_from_context(
    args: dict[str, Any], ctx: Context, edit_ctx=True
) -> dict[str, Any]:
    updated_kwargs = {}
    for k, v in args.items():
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
        if edit_ctx:
            # treat the arg as if it came from the cmdline
            ctx.set_parameter_source(k, ParameterSource.COMMANDLINE)
        updated_kwargs[k] = v

    if edit_ctx:
        # update value in context
        for k, v in updated_kwargs.items():
            ctx.params[k] = v
    return updated_kwargs


ARGS_YAMLPATH = "args.yml"


def app_with_yaml_support(app: Typer) -> Typer:
    def parse_yamlargs(
        ctx: Context,
        yamlpath: Path = Path(ARGS_YAMLPATH),
        yamlkeys: list[str] | None = None,
        yaml: bool = True,
    ) -> None:
        if not yaml:
            print("Skipping yaml support")
            return
        keys = yamlkeys
        if yamlkeys is None:
            if app.info.name:
                print(f"Using app name ({app.info.name}) as top-level yaml key")
                keys = [app.info.name]
            else:
                # TODO use module name as a fallback
                raise ValueError("App has no name to infer yaml key from")

        # get yamlargs
        do_trace = True if yamlkeys is None else False
        all_keys, yamlargs = yaml_nested_dict_get(
            yamlpath, keys=keys, trace_unknown_keys=do_trace
        )
        print(f"Args from {yamlpath.absolute()} under {all_keys}:")
        print(yamlargs)
        if not isinstance(yamlargs, dict):
            print("WARNING: args from yaml is not a dict. Assuming an empty dict")
            yamlargs = {}

        updated_kwargs = update_kwargs_from_context(yamlargs, ctx)
        ctx.obj = yamlargs
        if ctx.invoked_subcommand:
            print(f"parse_yamlargs: to be invoked: {ctx.invoked_subcommand}")
            return

        # get combined command
        cmd_names = list(map(lambda s: s.replace("!", ""), all_keys[1:]))
        funcs = app_funcs_from_keys(app, cmd_names)
        combined = combine_funcs(funcs)

        # call combined command with updated args
        ctx.invoked_subcommand = combined.__name__
        updated_kwargs["ctx"] = ctx
        ctx.invoke(combined, **updated_kwargs)

    # add_kwarg_updater_callbacks
    def updater(ctx: Context, funcs_dict: dict[str, Callable] | None = None):
        print("Running updater")
        if funcs_dict is None:
            raise ValueError
        if ctx.invoked_subcommand:
            print(f"To be invoked: {ctx.invoked_subcommand}")
            # ctx.invoke(funcs_dict[ctx.invoked_subcommand], **ctx.obj)

    def add_kwarg_updater_callbacks(tpr: Typer) -> None:
        print(f"Editing callbacks of {tpr}")
        cmds = [_ for _ in tpr.registered_commands if _.callback is not None]
        funcs = [_.callback for _ in cmds if _.callback is not None]
        names = [_.__name__ for _ in funcs]
        funcs_dict = dict(zip(names, funcs))
        pf = partial(updater, funcs_dict=funcs_dict)

        def upd(ctx: Context):
            pf(ctx)

        upd.__name__ = tpr.info.name if tpr.info.name else "updater"

        if tpr.registered_callback and tpr.registered_callback.callback:
            print(f"{tpr} already has callback")
            f = combine_funcs([upd, tpr.registered_callback.callback])
            app.callback(invoke_without_command=True)(f)
        else:
            print(f"{tpr} has no callback`")
            tpr.callback()(upd)

        for grp in tpr.registered_groups:
            subtpr = grp.typer_instance
            if not subtpr:
                continue
            add_kwarg_updater_callbacks(subtpr)

    add_kwarg_updater_callbacks(app)

    # prepend yaml parser callback
    funcs: list[Callable] = [parse_yamlargs]
    if app.registered_callback and app.registered_callback.callback:
        funcs.append(app.registered_callback.callback)
    yamlparsing_callback = combine_funcs(funcs)
    app.callback(invoke_without_command=True)(yamlparsing_callback)
    return app


Opt = Option


# REF (copilot) use this to refactor spiders
def opt(
    dtype: type, names: str | list[str], help: str | None, panel: str | None, **kwargs
) -> Annotated:
    if isinstance(names, str):
        names = [names]
    return Annotated[dtype, Option(*names, help=help, rich_help_panel=panel, **kwargs)]


app = Typer(name="cliutils")


subapp = Typer(name="subapp")


@subapp.command()
def subtest(ctx: Context, a=2, c=7):
    print()
    print(f"Running subtest cmd with: {ctx}")
    print(ctx.parent)
    print(ctx.params)
    print(f"Invoked: {ctx.invoked_subcommand}")


app.add_typer(subapp)


@app.callback()
def testcb(ctx: Context, z=8):
    print()
    print(f"Running test callback with {ctx}")
    print(ctx.params)
    print(f"Invoked: {ctx.invoked_subcommand}")
    ctx.invoke(subtest, ctx)


@app.command()
def test(ctx: Context, a=5, b="asvd"):
    print()
    print(f"Running test cmd with {ctx}")
    print(ctx.params)
    print(f"Invoked: {ctx.invoked_subcommand}")
    ctx.invoke(subtest, ctx)


if __name__ == "__main__":
    app_with_yaml_support(app)()
    # app()
