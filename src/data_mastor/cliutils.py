import argparse
from collections.abc import Callable, Sequence
from inspect import Parameter, Signature, signature
from pathlib import Path
from typing import Any, cast

import yaml
from click.core import ParameterSource
from rich import print
from typer import Context, Option, Typer


# NEXT replace with combine_funcs
def edit_signature(func, from_functions: list[Callable]):
    def dummy_func_with_ctx(ctx: Context):
        pass

    fullsig_dict: dict[str, Any] = {}
    for f in from_functions:
        sig = signature(cast(Callable[..., Any], f))
        fullsig_dict.update(sig.parameters)
    ctx_sig = {"ctx": signature(dummy_func_with_ctx).parameters["ctx"]}
    full_sig = Signature([*{**ctx_sig, **fullsig_dict}.values()])
    annots = {name: param.annotation for name, param in full_sig.parameters.items()}
    func.__signature__ = full_sig  # type: ignore
    func.__annotations__ = annots
    return func


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
            return [str(yamlpath)], {}
        else:
            return [str(yamlpath)], {}

    with open(yamlpath) as file:
        yamlpart = yaml.safe_load(file)
    if keys is None:
        return [str(yamlpath)], yamlpart
    if isinstance(keys, str):
        keys = [keys]
    for key in keys:
        if not isinstance(yamlpart, dict):
            exc = TypeError(f"Yamlpart before key '{key}' is not a dictionary")
            if doraise:
                raise exc
            elif debug:
                print(exc)
                return [str(yamlpath)], yamlpart
            else:
                return [str(yamlpath)], yamlpart
        if key not in yamlpart.keys():
            exc = KeyError(f"Key '{key}' was not found in yaml dict")
            if doraise:
                raise exc
            elif debug:
                print(exc)
                return [str(yamlpath)], yamlpart
            else:
                return [str(yamlpath)], yamlpart
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
    return [str(yamlpath)] + keys, yamlpart


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


def combine_funcs(
    funcs: Sequence[Callable], kwargs_updater: Callable | None = None
) -> Callable[..., None]:
    if not funcs:
        raise ValueError(f"Sequence of functions argument seems empty ({funcs})")

    def combined(**kwargs):
        if kwargs_updater is not None:
            params = signature(kwargs_updater).parameters
            kw = {k: v for k, v in kwargs.items() if k in params}
            print(f"\nCombined: calling '{kwargs_updater.__name__}' with: {kw}")
            updates = kwargs_updater(**kw)
            kwargs.update(updates)
        for f in funcs:
            params = signature(f).parameters
            kw = {k: v for k, v in kwargs.items() if k in params}
            print(f"\nCombined: calling '{f.__name__}' with: {kw}")
            f(**kw)

    names = []
    params = {}
    if kwargs_updater is not None:
        names.append(kwargs_updater.__name__)
        params.update(signature(kwargs_updater).parameters)
    for f in funcs:
        names.append(f.__name__)
        params.update(signature(f).parameters)
    combined.__name__ = "__".join(names)

    # sort by kind
    order = {
        Parameter.POSITIONAL_ONLY: 0,
        Parameter.POSITIONAL_OR_KEYWORD: 1,
        Parameter.VAR_POSITIONAL: 2,
        Parameter.KEYWORD_ONLY: 3,
        Parameter.VAR_KEYWORD: 4,
    }
    sorted_params = dict(sorted(params.items(), key=lambda item: order[item[1].kind]))

    combined.__signature__ = Signature(list(sorted_params.values()))  # type: ignore
    return combined


def yamlargs_from_params(
    yamlargs: dict[str, Any], ctx: Context, edit_ctx_values=True
) -> dict[str, Any]:
    updated_yamlargs = {}
    for k, v in yamlargs.items():
        if k not in ctx.params:
            print(f"Using yaml arg {k}={v} (unspecified arg)")
            updated_yamlargs[k] = v
            continue
        val = ctx.params[k]
        src = ctx.get_parameter_source(k)
        if src == ParameterSource.COMMANDLINE:
            print(f"Ignoring yaml arg {k}={v} (overriden by cmdline value: {val})")
            continue
        if v != val:
            print(f"Using yaml arg {k}={v} (ctx value from {src}: {val})")
        else:
            print(f"Using yaml arg {k}={v} (same as ctx value from {src})")
        # treat yaml arg as coming from the cmdline
        ctx.set_parameter_source(k, ParameterSource.COMMANDLINE)
        updated_yamlargs[k] = v

    if edit_ctx_values:
        for k, v in updated_yamlargs.items():
            ctx.params[k] = v
    return updated_yamlargs


ARGS_FILENAME = "args.yml"


def app_with_yaml_support(app: Typer, keys: list[str] | str | None = None) -> Typer:
    # use yaml only if no args were provided in the cmdline
    parser = argparse.ArgumentParser(add_help=False)
    _, unknown_args = parser.parse_known_args()
    if unknown_args:
        return app

    # read args from yaml
    print("Running app with YAML support since no args were provided")
    yamlpath = Path(ARGS_FILENAME)
    if keys is None and app.info.name is not None:
        print(f"Using app name ({app.info.name}) as top-level key")
        keys = app.info.name
    all_keys, yamlargs = yaml_nested_dict_get(yamlpath, keys=keys)
    print(f"Yamlargs from {all_keys}:")
    print(yamlargs)
    if not isinstance(yamlargs, dict):
        print("WARNING: yamlargs is not a dict. Assuming an empty dict instead.")
        yamlargs = {}

    # assimilate callbacks/callback from the app(s) into a single command
    cmd_names = list(map(lambda s: s.replace("!", ""), all_keys[2:]))
    funcs = app_funcs_from_keys(app, cmd_names)

    def parse_yamlargs(ctx: Context):
        return yamlargs_from_params(yamlargs, ctx)

    combined = combine_funcs(funcs, kwargs_updater=parse_yamlargs)

    # return the new app
    newapp = Typer()
    newapp.command()(combined)
    return newapp


Opt = Option
