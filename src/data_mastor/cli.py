import typer

from data_mastor import dbman

# SOMEDAY import these in a try-block instead, once scraper becomes an optional package
from data_mastor.cliutils import app_with_yaml_support
from data_mastor.scraper.spiders import ShopSrc

app = typer.Typer(
    name="dm",
    no_args_is_help=True,
    help="Main entry point to various data_mastor CLIs",
)
app.add_typer(dbman.app, help="Manage the database")
spiders_app = typer.Typer(name="sp")
# NEXT make this get the spider modules dynamically from scrapy SPIDERS_MODULE
for spidercls in [ShopSrc]:
    cmd_name = spidercls._cli_cmdname()

    cmd = spidercls.cli_app().registered_commands[0].callback
    if cmd is None:
        raise RuntimeError(f"Spidercls {spidercls} returns a NULL command")
    spiders_app.command(cmd_name)(cmd)
app.add_typer(spiders_app, help="Run spiders")

if __name__ == "__main__":
    app_with_yaml_support(app)()
