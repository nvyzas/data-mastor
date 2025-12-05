import copy
import logging
from collections.abc import Callable, Sequence
from functools import partial, wraps
from inspect import Signature, signature
from pathlib import Path
from typing import Annotated, Any

import yaml as pyyaml
from click.core import ParameterSource
from rich import print
from typer import Context, Option, Typer
from typer.models import TyperInfo

from data_mastor.utils import (
    FunctionFactory,
    _different,
    combine_funcs,
    nested_dict_get,
    replace_function_signature,
    sigpart,
)

# TYPER


def context_signature(key: str = "ctx"):
    def ctx_sig(ctx: Context):
        """A dummy function to hold the context argument."""

    new_param = signature(ctx_sig).parameters["ctx"].replace(name=key)
    return Signature([new_param])


# SOMEDAY Generalize this with editor and maker functions as args
class Tf:
    """Typer (App) Factory."""

    apps: dict[str | int, Typer] = {}

    def __call__(
        self,
        id_: str | int | None = None,
        force_new: bool = False,
        one_shot: bool = False,
        **kwargs,
    ) -> Typer:
        if id_ is None:
            id_ = _different(self.apps)
        id_ = str(id_)
        app = self.apps.get(id_)
        if force_new or app is None:
            app = make_typer(name=id_, **kwargs)
        else:
            app = edit_typer(app, one_shot=one_shot, **kwargs)
        if not one_shot:
            self.apps[id_] = app
        return app


def make_typer(
    name: str | None = None,
    cb: Callable | None = None,
    cmds: Sequence[Callable] | Callable | None = None,
    tprs: Sequence[Typer] | Typer | None = None,
    **kwargs,
) -> Typer:
    app = Typer(**kwargs, name=name)
    if cb is not None:
        app.callback()(cb)
    if cmds is not None:
        if not isinstance(cmds, Sequence):
            cmds = [cmds]
        for cmd in cmds:
            app.command()(cmd)
    if tprs is not None:
        if isinstance(tprs, Typer):
            tprs = [tprs]
        for tpr in tprs:
            app.add_typer(tpr)
    return app


def edit_typer(
    app: Typer,
    cb: Callable | None = None,
    one_shot: bool = False,
    funcs: list[Callable] | None = None,
    **kwargs,
) -> Typer:
    tpr = copy.deepcopy(app) if one_shot else app
    # edit callback
    if cb is not None:
        tpr.callback()(cb)
    # apply callables
    for f in funcs or []:
        f(tpr)
    # edit info
    for k, v in kwargs.items():
        setattr(tpr.info, k, v)
    return tpr


# MAYBE remove hardcoded names with lvl (since they are never used)
def traverse_typer(
    tpr: Typer,
    grp: TyperInfo | None = None,
    lvl: int = 0,
    callback_decorator: Callable[[Callable], Callable] | None = None,
    verbose: bool = False,
) -> None:
    _tprname = tpr.info.name or (grp.name if grp and grp.name else tpr.__module__)
    tprname = f"{_tprname}({lvl})"
    if tpr.registered_callback:
        if (cb := tpr.registered_callback.callback) is None:
            raise ValueError(f"{tpr} has a NULL callback")
        if callback_decorator is not None:
            tpr.registered_callback.callback = callback_decorator(cb)
            if verbose:
                print(f"Decorated callback '{tprname}'")
    for i, cmd in enumerate(tpr.registered_commands):
        if (cb := cmd.callback) is None:
            raise ValueError(f"'{tprname}' has a NULL command")
        cbname = cmd.name or cb.__name__
        cbname += f"({lvl})[{i}]"
        if callback_decorator is not None:
            cmd.callback = callback_decorator(cb)
            if verbose:
                print(f"Decorated command callback '{cbname}' of '{tprname}'")
    for i, grp in enumerate(tpr.registered_groups):
        grpname = grp.name or "grp"
        grpname += f"({lvl})[{i}]"
        if grp.typer_instance is None:
            raise ValueError(f"'{tprname}' has a group '{grpname}' with NULL app")
        traverse_typer(
            grp.typer_instance, grp, lvl + 1, callback_decorator=callback_decorator
        )


Opt = Option


# REF (copilot) use this to refactor spiders
def opt[T](
    dtype: type[T],
    names: str | Sequence[str],
    help: str | None,
    panel: str | None,
    **kwargs,
) -> Any:
    if not isinstance(names, Sequence):
        names = [names]
    return Annotated[dtype, Option(*names, help=help, rich_help_panel=panel, **kwargs)]


def printctx(ctx: Context):
    info = {
        "id": id(ctx),
        "params": ctx.params,
        "invoked": ctx.invoked_subcommand,
        "command": ctx.command,
        "command_path": ctx.command_path,
    }
    print(info)


def update_kwargs_from_context(
    kwargs: dict[str, Any],
    ctx: Context,
    update_ctx=False,
    inplace=False,
    ignored: list[str] | None = None,
    verbose: bool = True,
) -> dict[str, Any]:
    updated_kwargs = kwargs if inplace else {}
    ignored = ignored or []
    for k, v in kwargs.items():
        if k in ignored:
            if verbose:
                print(f"Ignoring arg {k} (ignore list)")
            continue
        if k not in ctx.params:
            if verbose:
                print(f"Using arg {k}={v} (unspecified)")
            updated_kwargs[k] = v
            continue
        val = ctx.params[k]
        src = ctx.get_parameter_source(k)
        if src == ParameterSource.COMMANDLINE:
            if verbose:
                print(f"Overriding arg {k}={v} (cmdline value: {val})")
            updated_kwargs[k] = val
            continue
        if verbose:
            print(f"Using arg {k}={v} ({src} value: {val}")
        updated_kwargs[k] = v
        if update_ctx:
            # treat the arg as if it came from the cmdline
            ctx.set_parameter_source(k, ParameterSource.COMMANDLINE)
            ctx.params[k] = v

    return updated_kwargs


# YAML

# SOMEDAY make these env vars
ARGS_YAMLPATH = "args.yml"
CTX_META_KEY_YAMLARGS = "yamlargs"
CTX_META_KEY_UNSPECIFIED = "unspecified"
CTX_META_KEY_DEBUG = "debug"


def read_yaml(path: str | Path):
    path = Path(path)
    with open(path) as file:
        content = pyyaml.safe_load(file)
    return content


def app_with_yaml_support(app: Typer) -> Typer:
    logger = logging.getLogger(app.info.name)

    def parse_args_from_yaml(
        ctx: Context,
        yamlpath: Path = Path(ARGS_YAMLPATH),
        yaml: bool = True,
        debug: bool = False,
    ) -> dict[str, Any]:
        logger.setLevel(logging.DEBUG if debug else logging.INFO)
        if not yaml:
            logger.warning("Yaml support is disabled. Assuming no yamlargs")
            return {}
        # read yaml file
        try:
            yamlargs = read_yaml(yamlpath)
        except Exception as exc:
            logger.warning(f"Failed to read yaml due to {exc}. Assuming no yamlargs")
            return {}
        # check result
        if not isinstance(yamlargs, dict):
            logger.warning(f"Yaml args {yamlargs} is not a dict. Assuming no yamlargs")
            return {}
        # store in ctx
        ctx.meta[CTX_META_KEY_YAMLARGS] = yamlargs
        ctx.meta[CTX_META_KEY_UNSPECIFIED] = {}
        ctx.meta[CTX_META_KEY_DEBUG] = debug
        return yamlargs

    def with_updated_kwargs(func):
        params = signature(func).parameters
        func_ctx_args = list(filter(lambda k: params[k].annotation is Context, params))
        if len(func_ctx_args) > 1:
            raise RuntimeError(f"More than one ctx args found in func: {func_ctx_args}")

        @wraps(func)
        def wrapper(ctx: Context, **kwargs):
            logger.debug(f"Running wrapper of {func.__name__} with {ctx} and {kwargs}")
            if yamlargs := ctx.meta.get(CTX_META_KEY_YAMLARGS):
                cmd = ctx.invoked_subcommand
                cmdstr = f"On subcmd={cmd}: " if cmd else ""
                keys = ctx.command_path.replace(".py", "").split(" ")
                args: dict[str, Any] = {}
                try:
                    used_keys, args = nested_dict_get(
                        yamlargs, keys=keys, raise_on_error=False
                    )
                except Exception as exc:
                    logger.warning(f"{cmdstr}Could not read yamlargs because of {exc}")
                else:
                    logging.debug(f"{cmdstr}Using args from keys {used_keys}:")
                    logging.debug(args)
                ignored = [invoked] if (invoked := cmd) else []
                debug = ctx.meta[CTX_META_KEY_DEBUG]
                updated = update_kwargs_from_context(
                    args, ctx, ignored=ignored, verbose=debug
                )
                kwargs.update(updated)
                unspecified = {k: v for k, v in updated.items() if k not in kwargs}
                ctx.meta[CTX_META_KEY_UNSPECIFIED][keys[-1]] = unspecified

            kw = {k: v for k, v in kwargs.items() if k in params}
            if func_ctx_args:
                kw[func_ctx_args[0]] = ctx
            logging.debug(f"Running wrapped {func.__name__} with {kw}")
            func(**kw)

        exclude = {func_ctx_args[0]: [func]} if func_ctx_args else None
        replace_function_signature(
            wrapper,
            [context_signature(), func],
            no_variadic=True,
            excluded_params=exclude,
        )
        return wrapper

    # edit copy of app
    app = copy.deepcopy(app)

    # decorate all typers' callbacks to work with updated kwargs
    traverse_typer(app, callback_decorator=with_updated_kwargs)

    # add parse callback to root typer
    if app.registered_callback:
        if not (cb := app.registered_callback.callback):
            raise RuntimeError(f"{app} has NULL callback")
        autoinvoke = app.registered_callback.invoke_without_command
        combined = combine_funcs([parse_args_from_yaml, cb])
        combined.__name__ = cb.__name__
        app.callback(invoke_without_command=autoinvoke)(combined)
    elif len(app.registered_commands) == 1:
        if not (cb := app.registered_commands[0].callback):
            raise RuntimeError(f"{app} has a (single) NULL command")
        combined = combine_funcs([parse_args_from_yaml, cb])
        combined.__name__ = cb.__name__
        app.registered_commands[0].callback = combined
    else:
        app.callback()(parse_args_from_yaml)
        if len(app.registered_commands) == 0:
            logging.warning("Typer has no commands, nor callback")
    return app


if __name__ == "__main__":
    t = Tf()
    ff = FunctionFactory()
    f = ff.__call__

    def sigs(ctxo: Context, a=1, b=1, c=1, d=1):
        pass

    s = partial(sigpart, sigs, "ctxo")

    # # app
    # app = Typer(name="cliutils")
    # app.callback(invoke_without_command=True)(f("cb1", printctx, s("a")))
    # app.command()(f("cmd1", printctx, s("a", "b")))

    # # subapp
    # subapp = Typer(name="subapp")
    # subapp.callback(invoke_without_command=True)(f("cb2", printctx, s("c")))
    # subapp.command()(f("cmd2", printctx, s("c", "d")))
    # app.add_typer(subapp)

    # run app
    # app_with_yaml_support(
    #     t("cliutils", invoke_without_command=True, cb=f("cb1", printctx, s("a")))
    # )()

    app_with_yaml_support(t("cliutils", invoke_without_command=True))()
