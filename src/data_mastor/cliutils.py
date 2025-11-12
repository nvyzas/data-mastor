from pathlib import Path
from typing import Any

import typer
import yaml
from click.core import ParameterSource


# DO replace all uses of this with yaml_get
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


def yamldict_get(
    yamlpath: str | Path,
    keys: list[str] | str | None = None,
    doraise: bool = True,
    debug: bool = False,
):
    exc: Exception

    yamlpath = Path(yamlpath)
    if not yamlpath.exists():
        exc = FileNotFoundError(f"File '{yamlpath}' does not exist")
        if doraise:
            raise exc
        elif debug:
            print(exc)
            return {}
        else:
            return {}

    with open(yamlpath) as file:
        yamlpart = yaml.safe_load(file)
    if keys is None:
        return yamlpart
    if isinstance(keys, str):
        keys = [keys]
    for key in keys:
        if not isinstance(yamlpart, dict):
            exc = TypeError(f"Yamlpart before key '{key}' is not a dictionary")
            if doraise:
                raise exc
            elif debug:
                print(exc)
                return {}
            else:
                return {}
        if key not in yamlpart.keys():
            exc = KeyError(f"Key '{key}' was not found in yaml dict")
            if doraise:
                raise exc
            elif debug:
                print(exc)
                return {}
            else:
                return {}
        yamlpart = yamlpart[key]

    return yamlpart


ARGS_FILENAME = "args.yml"


def run_yamlcmd(app: typer.Typer, key: str | None = None):
    yamlpath = Path(ARGS_FILENAME)
    yamlpart = yamldict_get(yamlpath, keys=key)
    cmdpath: list[str] = []
    while True:
        if not isinstance(yamlpart, dict):
            break
        cmd_keys = [k for k in yamlpart if "!" in k]
        if len(cmd_keys) > 1:
            raise KeyError(f"There are more than one keys with '!': {cmd_keys}")
        if len(cmd_keys) == 0:
            break
        cmdpath.append(cmd_keys[0])
        yamlpart = yamlpart[cmdpath[-1]]
    cmdpath[-1] = cmdpath[-1].replace("!", "")

    # register the yaml command alone
    cmd = None
    for reg_cmd in app.registered_commands:
        if reg_cmd.callback is None:
            raise RuntimeError(f"Encountered NULL callback: {reg_cmd.callback}")
        if reg_cmd.callback.__name__ == cmdpath[-1]:
            cmd = reg_cmd
            break
    else:
        raise RuntimeError(f"Typer app has no registed command named: {cmd}")
    app.registered_commands = [cmd]

    func = None
    if app.registered_callback is not None:
        func = app.registered_callback.callback

    # add/edit callback to parse the yaml args
    def parsing_callback(ctx: typer.Context):
        print("Running parsing_callback")
        parse_yamlargs(ctx, key=key, edit_ctx_values=True)
        if func is not None:
            print(f"Running ex-callback: {func.__name__}")
            func(ctx)

    app.callback()(parsing_callback)
    print(f"Running command from yaml {yamlpath}: {'->'.join(cmdpath)}")

    # run app
    app()


def parse_yamlargs(ctx: typer.Context, key: str | None = None, edit_ctx_values=True):
    cmdname = key or ctx.invoked_subcommand or ctx.command.name or None
    yamlargs = yamldict_get(ARGS_FILENAME, cmdname)
    print(f"Yamlargs in {Path(ARGS_FILENAME).absolute()} under key '{cmdname}':")
    print(yamlargs)

    # return and/or edit params
    params = ctx.params if edit_ctx_values else {}
    for k, v in yamlargs.items():
        if k not in ctx.params:
            print(f"Using yaml arg {k}={v} (unspecified arg)")
            params[k] = v
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
        params[k] = v
    return params


Opt = typer.Option
