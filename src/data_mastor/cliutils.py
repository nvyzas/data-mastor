from collections.abc import Callable
from functools import partial, wraps
from inspect import signature
from pathlib import Path
from typing import Annotated, Any

import yaml
from click.core import ParameterSource
from rich import print
from typer import Context, Option, Typer
from typer.models import TyperInfo

from data_mastor.utils import (
    _different,
    combine_funcs,
    mock_function_factory,
    nested_dict_get,
    replace_function_signature,
    sigpart,
)

# TYPER


def make_typer(
    name: str | None = None,
    cb: Callable | None = None,
    cmds: list[Callable] | Callable | None = None,
    tprs: list[Typer] | Typer | None = None,
) -> Typer:
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


# MAYBE remove hardcoded names with lvl (since they are never used)
def traverse_typer(
    tpr: Typer,
    grp: TyperInfo | None = None,
    lvl: int = 0,
    callback_decorator: Callable[[Callable], Callable] | None = None,
) -> None:
    _tprname = tpr.info.name or (grp.name if grp and grp.name else tpr.__module__)
    tprname = f"{_tprname}({lvl})"
    if tpr.registered_callback:
        if (cb := tpr.registered_callback.callback) is None:
            raise ValueError(f"{tpr} has a NULL callback")
        if callback_decorator is not None:
            tpr.registered_callback.callback = callback_decorator(cb)
            print(f"Decorated callback '{tprname}'")
    for i, cmd in enumerate(tpr.registered_commands):
        if (cb := cmd.callback) is None:
            raise ValueError(f"'{tprname}' has a NULL command")
        cbname = cmd.name or cb.__name__
        cbname += f"({lvl})[{i}]"
        if callback_decorator is not None:
            cmd.callback = callback_decorator(cb)
            print(f"Decorated command callback '{cbname}' of '{tprname}'")
    for i, grp in enumerate(tpr.registered_groups):
        grpname = grp.name or "grp"
        grpname += f"({lvl})[{i}]"
        if grp.typer_instance is None:
            raise ValueError(f"'{tprname}' has a group '{grpname}' with NULL app")
        traverse_typer(
            grp.typer_instance, grp, lvl + 1, callback_decorator=callback_decorator
        )


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
            app = make_typer(*args, **kwargs, name=str(id_))
            self.apps[id_] = app
            return app
        return self.apps.setdefault(id_, make_typer(*args, **kwargs, name=str(id_)))


Opt = Option


# REF (copilot) use this to refactor spiders
def opt(
    dtype: type, names: str | list[str], help: str | None, panel: str | None, **kwargs
) -> Annotated:
    if isinstance(names, str):
        names = [names]
    return Annotated[dtype, Option(*names, help=help, rich_help_panel=panel, **kwargs)]


def printctx(ctx: Context, **kwargs):
    info = {
        "id": id(ctx),
        "params": ctx.params,
        "invoked": ctx.invoked_subcommand,
        "command": ctx.command,
        "command_path": ctx.command_path,
    }
    print(info)


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
    update_ctx=False,
    inplace=False,
    ignored: list[str] | None = None,
) -> dict[str, Any]:
    updated_kwargs = kwargs if inplace else {}
    ignored = ignored or []
    for k, v in kwargs.items():
        if k in ignored:
            print(f"Ignoring arg {k} (ignore list)")
            continue
        if k not in ctx.params:
            print(f"Using arg {k}={v} (unspecified)")
            updated_kwargs[k] = v
            continue
        val = ctx.params[k]
        src = ctx.get_parameter_source(k)
        if src == ParameterSource.COMMANDLINE:
            print(f"Overriding arg {k}={v} (cmdline value: {val})")
            updated_kwargs[k] = val
            continue
        print(f"Using arg {k}={v} ({src} value: {val}")
        updated_kwargs[k] = v
        if update_ctx:
            # treat the arg as if it came from the cmdline
            ctx.set_parameter_source(k, ParameterSource.COMMANDLINE)
            ctx.params[k] = v

    return updated_kwargs


# YAML

ARGS_YAMLPATH = "args.yml"


def app_with_yaml_support(app: Typer) -> Typer:
    def parse_args_from_yaml(
        ctx: Context,
        yamlpath: Path = Path(ARGS_YAMLPATH),
        disabled: bool = False,
    ) -> dict[str, Any]:
        if disabled:
            print("WARNING: yaml support is disabled. Assuming no yamlargs")
            return {}
        if not yamlpath.exists():
            print(f"WARNING: yaml file {yamlpath} does not exist. Assuming no yamlargs")
            return {}
        # read yaml file
        with open(yamlpath) as file:
            yamlargs = yaml.safe_load(file)
        if not isinstance(yamlargs, dict):
            print(f"WARNING: yaml args {yamlargs} is not a dict. Assuming no yamlargs")
            return {}
        ctx.meta["yamlargs"] = yamlargs
        ctx.meta["unspecified"] = {}
        return yamlargs

    def with_updated_kwargs(func):
        @wraps(func)
        def wrapper(ctx: Context, **kwargs):
            print(f"Running wrapper of {func.__name__}")
            if yamlargs := ctx.meta.get("yamlargs"):
                keys = ctx.command_path.replace(".py", "").split(" ")
                args = {}
                try:
                    used_keys, args = nested_dict_get(yamlargs, keys=keys)
                    print(f"Using args from {used_keys}: {args}")
                except Exception as exc:
                    print(f"WARNING: {exc}")
                ignored = [invoked] if (invoked := ctx.invoked_subcommand) else []
                updated = update_kwargs_from_context(args, ctx, ignored=ignored)
                kwargs.update(updated)
                unspecified = {k: v for k, v in updated.items() if k not in kwargs}
                ctx.meta["unspecified"][keys[-1]] = unspecified
            kwargs["ctx"] = ctx

            kw = {k: v for k, v in kwargs.items() if k in signature(func).parameters}
            print(f"Running wrapped {func.__name__} with {kw}")
            func(**kw)

        replace_function_signature(wrapper, [wrapper, func], no_variadic=True)
        return wrapper

    # decorate all typer callbacks to work with updated kwargs
    traverse_typer(app, callback_decorator=with_updated_kwargs)

    # root callback
    root_callback = parse_args_from_yaml
    if app.registered_callback and app.registered_callback.callback:
        funcs = [parse_args_from_yaml, app.registered_callback.callback]
        root_callback = combine_funcs(funcs)
    app.callback(invoke_without_command=True)(root_callback)
    return app


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


if __name__ == "__main__":
    f = mock_function_factory

    def sigs(ctx: Context, a=1, b=1, c=1, d=1):
        pass

    s = partial(sigpart, sigs, "ctx")

    # app
    app = Typer(name="cliutils")
    app.callback(invoke_without_command=True)(f("cb1", printctx, s("a")))
    app.command()(f("cmd1", printctx, s("a", "b")))

    # subapp
    subapp = Typer(name="subapp")
    subapp.callback(invoke_without_command=True)(f("cb2", printctx, s("c")))
    subapp.command()(f("cmd2", printctx, s("c", "d")))
    app.add_typer(subapp)

    app_with_yaml_support(app)()
    # app()
