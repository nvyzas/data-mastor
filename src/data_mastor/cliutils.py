import os
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


def yaml_get(yamlpath: str | Path, keys: list[str] | str = "", doraise: bool = False):
    yamlpath = Path(yamlpath)
    if yamlpath.is_file():
        with open(yamlpath) as file:
            yamlpart = yaml.safe_load(file)
        if not keys:
            return yamlpart
        if isinstance(keys, str):
            keys = [keys]
        for key in keys:
            if not isinstance(yamlpart, dict):
                msg = f"Yamlpart is {type(yamlpart)}, not a dictionary"
                if doraise:
                    raise TypeError(msg)
                else:
                    print(msg)
                    return {}
            if key not in yamlpart.keys():
                msg = f"Key '{key}' was not found in yaml dict"
                if doraise:
                    raise KeyError(msg)
                else:
                    print(msg)
                    return {}
            yamlpart = yamlpart[key]
        return yamlpart
    else:
        msg = f"File '{yamlpath}' was not found"
        if doraise:
            raise FileNotFoundError(msg)
        else:
            print(msg)
            return {}


def yaml_go_cmd(yamlpath):
    yamlpart = yaml_get(yamlpath)
    if not isinstance(yamlpart, dict):
        raise TypeError(f"{yamlpart} is not a dictionary")
    keys = list(yamlpart.keys())
    index = keys.index("go")
    return keys[index + 1]


YAML_PATH_ENVVAR = "YAML_PATH"


def parse_yamlargs(ctx: typer.Context, key: str | None = None, edit_ctx_values=True):
    yamlpath = Path(os.environ.get(YAML_PATH_ENVVAR, "args.yml"))
    cmdname = key or ctx.invoked_subcommand or ctx.command.name or ""
    yamlargs = yaml_get(yamlpath, cmdname, doraise=False)
    if not yamlargs:
        print(f"There are no yamlargs in {yamlpath.absolute()} under key '{cmdname}'")
        return {}
    print(f"Found yamlargs in {yamlpath.absolute()} under key '{cmdname}':")
    print(yamlargs)

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
