from pathlib import Path
from typing import Any

import typer
import yaml
from click.core import ParameterSource
from rich import print


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


def nested_yaml_dict_get(
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
    print(f"Yamlpart from {keys}:")
    print(yamlpart)
    return [str(yamlpath)] + keys, yamlpart


def yamlargs_with_params(
    yamlargs: dict[str, Any], ctx: typer.Context, edit_ctx_values=True
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


ARGS_FILENAME = "args.yml"


def run_yamlcmd(app: typer.Typer, keys: list[str] | str | None = None) -> None:
    yamlpath = Path(ARGS_FILENAME)
    all_keys, yamlargs = nested_yaml_dict_get(yamlpath, keys=keys)
    if not isinstance(yamlargs, dict):
        print(f"Warning: yamlargs ({yamlargs}) is not a dict. Using empty dict.")
        yamlargs = {}
    cmd_name = all_keys[-1].replace("!", "")

    # register the yaml command alone
    cmd = None
    for reg_cmd in app.registered_commands:
        if reg_cmd.callback is None:
            raise RuntimeError(f"Encountered NULL callback: {reg_cmd.callback}")
        if reg_cmd.callback.__name__ == cmd_name:
            cmd = reg_cmd
            break
    else:
        raise RuntimeError(f"Typer app has no registed command named: {cmd}")
    app.registered_commands = [cmd]

    # add callback to parse the yaml args, incorporating the existing callback (if any)
    func = None
    if app.registered_callback is not None:
        func = app.registered_callback.callback

    def parsing_callback(ctx: typer.Context):
        print("Running parsing_callback")
        ctx.obj = {}
        ctx.obj["updated_yamlargs"] = yamlargs_with_params(
            yamlargs, ctx, edit_ctx_values=True
        )
        if func is not None:
            print(f"Running ex-callback: {func.__name__}")
            func(ctx)

    app.callback()(parsing_callback)
    print(f"Running command {cmd_name}")

    # run app
    app()


Opt = typer.Option
