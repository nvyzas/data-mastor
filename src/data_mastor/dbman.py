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
from rich import print as rprint
from sqlalchemy import Engine, MetaData, create_engine

from data_mastor.cliutils import get_yamldict_key
from data_mastor.scraper.models import Base

# typer app
app = typer.Typer(invoke_without_command=True)

# engine
_engine: None | Engine = None


def get_db_url():
    return os.environ["DB_URL"]


def import_extension_module():
    """Import project-specific model subclasses so their mappers are registered."""
    extension_module: str | None = os.environ.get("DB_MODULE", None)
    if extension_module is not None:
        importlib.import_module(extension_module)


def get_engine(**kwargs):
    import_extension_module()
    global _engine
    if _engine is None:
        _engine = create_engine(get_db_url(), **kwargs)
    return _engine


# now utility function
def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


# DATABASE MANAGEMENT


def _tables_dict(metadata_obj: MetaData) -> dict[str, dict[str, str]]:
    ret = {}
    tablesdict = metadata_obj.tables
    for tablename, table in tablesdict.items():
        ret[tablename] = {col.name: str(col.type) for col in table.columns}
    return ret


@app.callback()
def callback(ctx: typer.Context):
    engine = get_engine()
    ctx.obj = engine


@app.command()
def dbmd(ctx: typer.Context, echo=True):
    engine = ctx.obj
    db_metadata = MetaData()
    db_metadata.reflect(bind=engine)
    db_md = _tables_dict(db_metadata)
    if echo:
        rprint(db_md)
    return db_md


@app.command()
def srcmd(echo=True):
    src_metadata = _tables_dict(Base.metadata)
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
def migrate(ctx: typer.Context, yamlargs_path: Path = Path("args.yml")):
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
    engine = ctx.obj

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
    data = {}
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

    # backup
    backup_dirpath = Path("backup")
    backup_filename = sqlite_db_path.split(".")[0] + "_" + _now() + ".bak.db"
    backup_filepath = backup_dirpath / backup_filename
    shutil.copy2(sqlite_db_path, backup_filepath)
    print(f"Created database backup: {str(backup_filepath.absolute())}")

    # recreate
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    print(f"Recreated database: {sqlite_db_path}")

    # load data using the new schema
    exc_occured = False
    for tname, df in data.items():
        if tname == "alembic_version":
            continue
        try:
            df.to_sql(tname, engine, index=False, if_exists="append")
        except Exception as exc:
            exc_occured = True
            print(f"Failed to restore data into table '{tname}' due to {exc}")
            break
        print(f"Successfully restored data into table '{tname}'")

    if exc_occured:
        error_filepath = backup_filepath.with_stem(backup_filepath.stem + ".err")
        shutil.move(sqlite_db_path, error_filepath)
        shutil.move(backup_filepath, sqlite_db_path)
        print("Replaced database with the backup because an exception occured")
        print(f"Erroneous database path: '{error_filepath}'")
        return

    if dont_store:
        dryrun_filepath = backup_filepath.with_stem(backup_filepath.stem + ".dr")
        shutil.move(sqlite_db_path, dryrun_filepath)
        shutil.move(backup_filepath, sqlite_db_path)
        print("Replaced database with the backup because it's a dry-run")
        print(f"Dry-run database path: '{dryrun_filepath}'")
        return


if __name__ == "__main__":
    app()
