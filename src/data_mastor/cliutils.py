import inspect
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import yaml
from click.core import ParameterSource
from rich import print
from typer import Context, Option, Typer


def edit_signature(func, from_functions: list[Callable]):
    def dummy_func_with_ctx(ctx: Context):
        pass

    fullsig_dict: dict[str, Any] = {}
    for f in from_functions:
        sig = inspect.signature(cast(Callable[..., Any], f))
        fullsig_dict.update(sig.parameters)
    ctx_sig = {"ctx": inspect.signature(dummy_func_with_ctx).parameters["ctx"]}
    full_sig = inspect.Signature([*{**ctx_sig, **fullsig_dict}.values()])
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
                raise KeyError(f"There are more than one keys with '!': {marked_keys}")
            if len(marked_keys) == 0:
                break
            unknown_keys.append(marked_keys[0])
            yamlpart = yamlpart[unknown_keys[-1]]
        keys += unknown_keys
    return [str(yamlpath)] + keys, yamlpart


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
        if ctx.get_parameter_source(k) == ParameterSource.COMMANDLINE:
            print(f"Ignoring yaml arg {k}={v} (overriden by cmdline value: {val})")
            continue
        # treat yaml arg as coming from the cmdline
        ctx.set_parameter_source(k, ParameterSource.COMMANDLINE)
        if v != val:
            print(f"Using yaml arg {k}={v} (different ctx value={val})")
        else:
            print(f"Using yaml arg {k}={v} (same as ctx value)")
        updated_yamlargs[k] = v

    if edit_ctx_values:
        for k, v in updated_yamlargs.items():
            ctx.params[k] = v
    return updated_yamlargs


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

    # check if the current app (tpr variable) has at least one command
    print(f"Looking for cmd at iteration '{i}'")
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
        funcs.append(key_cmds[0].callback)  # type: ignore
        return funcs

    # the last key corresponds to an app, look for a single command
    if len(tpr.registered_commands) > 1:
        raise ValueError(f"App({i}) has multiple commands but no cmdkey was given")
    if tpr.registered_groups:
        raise ValueError(f"App({i}) has groups but no cmdkey was given")
    if tpr.registered_commands[0].callback is None:
        raise ValueError(f"App({i}) has a single command but it is NULL")
    funcs.append(tpr.registered_commands[0].callback)
    return funcs


ARGS_FILENAME = "args.yml"


def app_with_yaml_support(app: Typer, keys: list[str] | str | None = None) -> Typer:
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

    group_names = list(map(lambda s: s.replace("!", ""), all_keys[1:-1]))
    # find the command marked in the yaml
    # typer_info = app
    # for key in group_names:
    #     typer_obj = typer_obj.registered_groups
    cmd_name = all_keys[-1].replace("!", "")
    # cmd = None
    # for reg_cmd in app.registered_commands:
    #     if reg_cmd.callback is None:
    #         raise RuntimeError(f"Encountered NULL callback: {reg_cmd.callback}")
    #     if reg_cmd.name == cmd_name or reg_cmd.callback.__name__ == cmd_name:
    #         cmd = reg_cmd
    #         break
    # else:
    #     raise RuntimeError(f"Typer app has no registered command named: {cmd_name}")
    # app.registered_commands = [cmd]
    # print(f"Command name: {cmd.name or reg_cmd.callback.__name__}")

    # add callback to parse the yaml args, incorporating the existing callback (if any)

    ex_callback = (
        app.registered_callback.callback
        if app.registered_callback is not None
        else None
    )

    def parser(ctx: Context):
        print(f"Running parsing_callback with params: {ctx.params}")
        ctx.obj = {}
        ctx.obj["yamlargs"] = yamlargs_from_params(yamlargs, ctx, edit_ctx_values=False)

    if ex_callback is not None:
        print("1")

        def combined_callback(ctx: Context, **kwargs):
            parser(ctx)
            print(f"Running ex-callback with params: {ctx.params}")
            kw = {k: v for k, v in kwargs.items() if k in ex_callback.__annotations__}
            ex_callback(ctx, **kw)

        # app.callback()(edit_signature(combined_callback, [ex_callback]))
    else:
        print("2")
        # app.callback()(parser)

    return app


Opt = Option
