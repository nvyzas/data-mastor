import argparse
import importlib
import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from types import ModuleType
from typing import Any

import pandas as pd
import typer
from deepdiff import DeepDiff
from deepdiff.helper import COLORED_VIEW
from pandas import DataFrame
from rich import print as rprint
from sqlalchemy import Engine, MetaData, create_engine
from sqlalchemy.orm.decl_api import DeclarativeBase

from data_mastor.cliutils import app_with_yaml_support, get_yamldict_key

# typer app
app = typer.Typer(name="db", no_args_is_help=True, add_completion=False)

# engine
_engine: None | Engine = None


def _import_extension_module() -> ModuleType | None:
    """Import project-specific model subclasses so that their mappers are registered."""
    extension_module: str | None = os.environ.get("DB_MODULE", None)
    if extension_module is not None:
        print(f"Importing extension module: {extension_module}")
        return importlib.import_module(extension_module)


def _get_declarative_base_class() -> type[DeclarativeBase]:
    module = _import_extension_module()
    keys = ["Base"] + [_ for _ in dir(module) if not _.startswith("__")]
    for k in keys:
        attr = getattr(module, k)
        if not hasattr(attr, "metadata"):
            continue
        if isinstance(attr, type) and issubclass(attr, DeclarativeBase):
            return attr
    raise RuntimeError(f"There is no declarative base class for the model in {module}")


def _get_db_url() -> str:
    db_url = os.environ.get("DB_URL")
    if db_url is None:
        db_url = "sqlite:///:memory:"
        print("WARNING: DB_URL env var is not set. Using in-memory database")
    return db_url


def get_engine(**kwargs) -> Engine:
    _import_extension_module()
    global _engine
    if _engine is None:
        _engine = create_engine(_get_db_url(), **kwargs)
        print(f"Using engine: {_engine.url}")
    return _engine


# DATABASE MANAGEMENT


def _tables_dict(metadata_obj: MetaData) -> dict[str, dict[str, str]]:
    ret = {}
    tablesdict = metadata_obj.tables
    for tablename, table in tablesdict.items():
        ret[tablename] = {col.name: str(col.type) for col in table.columns}
    return ret


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def _create_backup(db_filepath: Path) -> Path:
    backup_filename = db_filepath.stem + "_" + _now() + ".bak.db"
    backup_filepath = Path("backups") / backup_filename
    shutil.copy2(db_filepath, backup_filepath)
    print(f"Created database backup: {str(backup_filepath.absolute())}")
    return backup_filepath


def _restore_backup(
    backup_filepath: Path, db_filepath: Path, extension_prefix: str = ""
) -> None:
    target_filepath = backup_filepath.with_stem(backup_filepath.stem + extension_prefix)
    shutil.move(db_filepath, target_filepath)
    shutil.move(backup_filepath, db_filepath)
    print(f"Recreated database backup: {target_filepath.absolute()}")
    print(f"Restored database from previous backup: {backup_filepath.absolute()}")


def _try_safely(func, ctx: typer.Context) -> None:
    db_filepath = ctx.obj["db_filepath"]
    backup = ctx.params["backup"]
    write_db = ctx.params["write_db"]
    if not write_db and not backup:
        raise ValueError("Dry run (no write_db) requires backup")

    backup_filepath = _create_backup(db_filepath) if backup else None

    print(f"Running {func.__name__}")
    try:
        func()
    except Exception as exc:
        if not backup:
            raise
        print(f"Exception occured: {exc}")
        if backup_filepath is not None:
            _restore_backup(backup_filepath, db_filepath, ".err")
    else:
        if not write_db and backup_filepath is not None:
            _restore_backup(backup_filepath, db_filepath, ".dryrun")


@app.callback()
def callback(ctx: typer.Context):
    # init ctx obj to be shared
    ctx.obj = ctx.obj or {}

    # get engine
    ctx.obj["engine"] = get_engine()

    # get db filepath
    db_url = str(ctx.obj["engine"].url)
    if not db_url.startswith("sqlite:///"):
        raise RuntimeError(f"Invalid db url: {db_url}")
    db_filepath = Path(db_url.replace("sqlite:///", "", count=1))
    ctx.obj["db_filepath"] = db_filepath


@app.command()
def recreate(ctx: typer.Context, backup=True, write_db=False):
    engine = ctx.obj["engine"]

    def _recreate():
        print("Recreating")
        # Base.metadata.drop_all(engine)
        # Base.metadata.create_all(engine)

    _try_safely(_recreate, ctx)


@app.command()
def dbmd(ctx: typer.Context, echo=True):
    engine = ctx.obj["engine"]
    db_metadata = MetaData()
    db_metadata.reflect(bind=engine)
    db_md = _tables_dict(db_metadata)
    if echo:
        rprint(db_md)
    return db_md


@app.command()
def srcmd(echo=True):
    basecls = _get_declarative_base_class()
    src_metadata = _tables_dict(basecls.metadata)
    if echo:
        rprint(src_metadata)
    return src_metadata


@app.command()
def diff(ctx: typer.Context, echo=True):
    db_md = dbmd(ctx, echo=False)
    src_md = srcmd(echo=False)
    diffs = DeepDiff(
        db_md,
        src_md,
        ignore_order=True,
        view=COLORED_VIEW,
        exclude_paths=["root['alembic_version']"],
    )
    if echo:
        print(diffs)
    return db_md, src_md, diffs


@app.command()
def migrate(ctx: typer.Context, backup=True, write_db=False):
    """Helper for database migration/creation.

    Useful for databases with limited migration capabilities e.g., sqlite.\n
    Supported operations:\n
    - table rename -> use args.yml:renames:oldtablename:oldtablename:newtablename\n
    - column rename -> use args.yml:renames:tablename:oldcolumnname:newcolumnname\n
    - table removal -> detected automatically by comparing db with this file\n
    - column removal -> detected automatically by comparing db with this file\n
    - table add -> handled by metadata.create_all\n
    - column add -> handled metadata.create_all\n
    - table change constraints -> handled by metadata.create_all\n
    - column change type -> handled by metadata.create_all\n
    For other kinds of operations:\n
    https://alembic.sqlalchemy.org/en/latest/autogenerate.html
    """
    # parse args
    args = get_yamldict_key(yamlargs_path, "db")
    renames = args.get("renames", {}) or {}
    dont_store = args.get("dont_store", True) or True
    args_dict = {"renames": renames, "dont_store": dont_store}
    print(f"Args: {args_dict}")

    # get engine
    engine = ctx.obj["engine"]

    # calculate database metadata, source metadata, and their diffs
    db_md, src_md, diffs = diff(ctx, echo=False)

    # determine removed tables/columns
    removed_tables = set()
    removed_columns: dict[str, Any] = {}
    for item in diffs["dictionary_item_removed"]:
        parts = re.findall(r"\[(.*?)\]", item)
        if len(parts) == 1:
            table = parts[0].replace("'", "")
            if table in renames and table in renames[table]:
                print(f"Table '{table}' will be renamed to {renames[table]}")
            else:
                removed_tables.add(table)
                print(f"Table '{table}' will be removed")
        elif len(parts) == 2:
            table = parts[0].replace("'", "")
            col = parts[1].replace("'", "")
            if table in renames and col in renames[table]:
                newcol = renames[table][col]
                print(f"Column '{col}' of table '{table}' will be renamed to {newcol}")
            else:
                removed = removed_columns.setdefault(table, set())
                removed.add(col)
                print(f"Column '{col}' of table '{table}' will be removed")
        else:
            raise RuntimeError(f"Unexpected length of diff parts: {parts}")

    # store existing table data first, so that next steps don't run in case of error
    print("Storing existing data")
    data: dict[str, DataFrame] = {}
    for tname in db_md:
        if tname == "alembic_version":
            print(f"Skipping table '{tname}'")
            continue
        # check if table is marked as removed
        if tname in removed_tables:
            print(f"Skipping removed table '{tname}'")
            continue
        # check renames for the specific table
        table_renames = renames.pop(tname, {})
        new_tname = table_renames.pop(tname, tname)
        renamed_to = " " if tname == new_tname else f" (renamed to '{new_tname}') "
        # make sure tablename is a string (in case of rename)
        if not isinstance(new_tname, str):
            print(f"New tablename should be a string, not {type(new_tname)}")
        # make sure table exists in the new schema
        if new_tname not in src_md:
            raise ValueError(f"Table '{tname}'{renamed_to}not in schema")
        # determine included columns (those that were not removed)
        included_cols = db_md[tname].keys() - removed_columns.get(tname, {})
        # determine datetime columns
        date_cols = [
            col
            for col, coltype in db_md[tname].items()
            if col in included_cols and coltype == "DATETIME"
        ]
        # read data
        df = pd.read_sql_table(tname, engine, parse_dates=date_cols)
        # keep only included columns
        df = df[list(included_cols)]
        if df.empty:
            print(f"Skipping empty table '{tname}'")
            continue
        # handle column renames
        column_renames = {k: v for k, v in table_renames.items()}
        if column_renames:
            df.rename(columns=column_renames, inplace=True)
        # store data to dict
        data[new_tname] = df
        num_cols = len(df.columns)
        print(f"Stored data ({num_cols} columns) of table '{tname}'{renamed_to}")
    print(f"Unused renames from yaml: {renames}")

    # do the migration
    def _migrate():
        recreate(ctx)
        for tname, df in data.items():
            if tname == "alembic_version":
                continue
            df.to_sql(tname, engine, index=False, if_exists="append")
            print(f"Restored data into table '{tname}'")

    _try_safely(_migrate, ctx)


if __name__ == "__main__":
    app_with_yaml_support(app)()
