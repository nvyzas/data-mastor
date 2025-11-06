import inspect
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, TypeGuard

import typer
import yaml
from click.core import ParameterSource
from rich import print
from scrapy import Spider
from scrapy.crawler import CrawlerProcess
from scrapy.exceptions import CloseSpider
from scrapy.http import Response
from scrapy.settings import SETTINGS_PRIORITIES, Settings
from scrapy.utils.project import get_project_settings

from data_mastor.cliutils import Opt, parse_yamlargs, yaml_get
from data_mastor.scraper.middlewares import PrivacyCheckerDlMw, ResponseSaverSpMw
from data_mastor.scraper.pipelines import TIMESTAMP_FMT, ListingStorer, SourceStorer
from data_mastor.scraper.utils import DLMW_KEY, DLMWBASE_KEY, between_middlewares

LATIN_ALPHABET = "AaBbGgDdEeZzHhJjIiKkLlMmNnXxOoPpRrSssTtUuFfQqYyWw"

USED_ARGS_FILENAME = "used_args.yml"

# REFACTOR: use a class to manage the timestamp instead
timestamp: str = ""


def set_timestamp():
    global timestamp
    if timestamp != "":
        raise RuntimeError(f"Timestamp ({timestamp}) is supposed to be set only once!")
    timestamp = datetime.now().strftime(TIMESTAMP_FMT)


set_timestamp()


class Baze(Spider):
    # CLI
    # required in get_yaml_key(cls.name, ...)
    name = "baze"
    # required for validation/prioritization of args
    _specified_args = set()  # used by typer option callbacks => init here (before ctx)
    # make settings and spiderargs accessible to typer callback AND typer command
    _settings: dict[str, Any] = {}
    _spiderargs: dict[str, Any] = {}
    # for testing the cli, by exiting before crawling
    _test_cli: bool
    # serves as a single source of truth for default spiderarg/setting values
    # applied in both: scrapy crawl CLI (__init__/from_crawler), and custom CLI (_cli)
    # REFACTOR: replace specs with pydantic classes
    sett_specs: dict[str, Any] = {
        "OUT_DIR": f"out/{name}/{timestamp}",
        "DONT_STORE": False,
        "NOW": timestamp,
    }
    sparg_specs: dict[str, Any] = {"url": None, "save_html": False}

    # default values for spiderargs (**kwargs) are defined here to stay compatible
    def __init__(self, name=None, **kwargs):
        # run default init first to assign name, spiderargs, and start_urls to self
        super().__init__(name, **kwargs)

        # check if url arg was given
        try:
            self.url
        except AttributeError:
            self.url = self.sparg_specs["url"]
            print(f"Using default value: url={self.url}")
        if self.url:
            if self.start_urls:
                print(f"Overriding default (class) start-urls ({self.start_urls})")
            self.start_urls = [self.url]
        # check if any start-url is a local file
        self._local_mode = False
        for i, url in enumerate(self.start_urls):
            if url.startswith("http://") or url.startswith("https://"):
                if self._local_mode:
                    raise RuntimeError("Non-local URL in local-mode")
                continue
            f = Path(url.replace("file://", ""))
            if not f.is_file():
                raise FileExistsError(f"Local file '{f}' does not exist")
            url = "file://" + str(f.absolute())
            if not self._local_mode:
                print(f"Found local URL '{url}' => Enabling local-mode")
                self._local_mode = True
            self.start_urls[i] = url
        print(f"Start-urls: {self.start_urls}")

        # make sure all local URLs point to a single directory
        self.local_dir: Path | None = None
        if self._local_mode:
            for url in self.start_urls:
                dir = Path(url.split("file://")[-1]).parent
                if self.local_dir is not None and dir != self.local_dir:
                    raise RuntimeError(
                        f"Not all of {self.start_urls} belong to the same dir"
                    )
                self.local_dir = dir

        # check if save_html arg was given
        try:
            self.save_html
        except AttributeError:
            self.save_html = self.sparg_specs["save_html"]
            print(f"Using default value: save_html={self.save_html}")

        # check if html_namer function is defined
        if self.save_html or self.local_mode:
            self.html_namer_calls = 1
            try:
                self.html_namer
            except AttributeError:
                raise AttributeError(
                    "save_html is on but html_namer spidercls function is undefined"
                )

    @property
    def local_mode(self) -> bool:
        """Whether the spider is scraping locally.

        Enabled if any of the start_urls is a local file. All other start_urls (if any)
        are also expected to be local. All start_urls are converted to the format
        'file:///absolute/url'
        """
        return self._local_mode

    @staticmethod
    def split_url(url: str):
        """Get the first and last part of a url."""
        parts = url.rstrip(".html").rstrip("/").split("/")
        base = "/".join(parts[:-1]) + "/"
        tip = parts[-1]
        return base, tip

    def localize(self, s) -> str:
        """Calculate the localized url."""
        return s.replace("?", "_").replace("=", "")

    def html_namer(self, response: Response) -> str:
        """Returns the name of the html file to save (if self.save_html is enabled)."""
        base, tip = self.split_url(response.url)
        if not base.startswith("file://"):
            tip = self.localize(tip) + ".html"
        return tip

    def next_local_url(self, key):
        if not self.local_mode:
            raise CloseSpider("next_local_url was called in non-local mode")
        results = [path for path in self.local_dir.iterdir() if key == path.name]
        if len(results) != 1 or len(results) == 0:
            raise CloseSpider(f"Number of matched results ({results}) is not 1")
        result = results[0]
        if not result.is_file():
            raise CloseSpider(f"Result ({result}) is not a file")
        next_url = "file://" + str(result)
        return next_url

    @classmethod
    def _all_baze_classes(cls) -> list[type["Baze"]]:
        def is_list_of_baze_spiders(spiders: list[type]) -> TypeGuard[list[type[Baze]]]:
            return all(issubclass(s, Baze) for s in spiders)

        classes = []
        for cls_ in cls.__mro__[::-1]:
            if issubclass(cls_, Baze):
                classes.append(cls_)
        if not is_list_of_baze_spiders(classes):
            raise TypeError(f"classes variable contains non-Baze classes: {classes}")

        return classes

    @classmethod
    def all_sparg_specs(cls):
        all_spargs = {}
        for cls_ in cls._all_baze_classes():
            all_spargs.update(cls_.sparg_specs)
        return all_spargs

    @classmethod
    def all_sett_specs(cls):
        all_setts = {}
        for cls_ in cls._all_baze_classes():
            all_setts.update(cls_.sett_specs)
        return all_setts

    @classmethod
    def calc_out_dir(cls, subdir: str):
        return Path(f"out/{cls.name}/{subdir}")

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """Apply settings from lower to higher precedence: project -> custom -> dynamic -> specs -> cli / _settings (testing setup)"""
        # run default from_crawler first to init the spider and its settings attribute
        spider = super().from_crawler(crawler, cls.name, **kwargs)

        # calculate spec settings from all bases' specs
        spec_settings = cls.all_sett_specs()

        # update with settings from specifications
        spider.settings.update(spec_settings, priority="spider")

        # update with dynamic settings
        dynamic_settings: dict[str, Any] = {}
        out_dir = Path(spider.settings["OUT_DIR"])
        dynamic_settings["LOG_FILE"] = str(out_dir / "run.log")
        dynamic_settings["FEEDS"] = {str(out_dir / "feed.json"): {"format": "json"}}
        overriden = []
        for k in dynamic_settings:
            if k in spec_settings:
                v = spec_settings[k]
                print(f"Dynamic setting '{k}' will be overriden by spec value '{v}'")
                overriden.append(k)
            if spider.settings.getpriority(k) == SETTINGS_PRIORITIES["cmdline"]:
                v = spider.settings[k]
                print(f"Dynamic setting '{k}' will be overriden by cmdline value '{v}'")
                overriden.append(k)
        [dynamic_settings.pop(k) for k in overriden]
        spider.settings.update(dynamic_settings, priority="spider")

        # apply middlewares
        dlmw_base = spider.settings[DLMWBASE_KEY]
        dlmw = spider.settings[DLMW_KEY]

        # Only add PrivacyCheckerDLMW if not in local mode
        if not spider._local_mode:
            pos = between_middlewares(
                {**dlmw_base, **dlmw},
                [
                    "DefaultHeadersMiddleware",
                    "UserAgentMiddleware",
                    "RandomUserAgentMiddleware",
                ],
            )
            dlmw[PrivacyCheckerDlMw] = pos

        if spider.save_html:
            dlmw[ResponseSaverSpMw] = 950

        # effectively disable OffsiteDownloadMiddleware if scraping locally
        if spider._local_mode:
            cls.allowed_domains = []
            print(f"Allowed domains were set to: {cls.allowed_domains}")

        # conditionally create out_dir and write the used args yaml file
        def _pathstr(path: str | Path | None):
            if path is None:
                return ""
            return str(Path(path))

        logpath = spider.settings["LOG_FILE"]
        log_to_out_dir = _pathstr(logpath).startswith(str(out_dir))
        feedpaths = spider.settings["FEEDS"].keys()
        feeds_to_out_dir = any(
            map(lambda p: _pathstr(p).startswith(str(out_dir)), feedpaths)
        )
        if spider.save_html or log_to_out_dir or feeds_to_out_dir:
            out_dir.mkdir(parents=True, exist_ok=False)
            print(f"Created out dir: {out_dir}")
            cli_settings = {
                k: v
                for k, v in spider.settings.items()
                if spider.settings.getpriority(k) == SETTINGS_PRIORITIES["cmdline"]
            }
            used_args_dict = {**cli_settings, **kwargs}
            with open(out_dir / USED_ARGS_FILENAME, "w") as file:
                yaml.dump({spider.name: used_args_dict}, file, default_flow_style=False)
        # return
        return spider

    @staticmethod
    def _verbose_update(
        updated: dict, updater: dict, updater_name="", overwrite=True
    ) -> None:
        for key, value in updater.items():
            if key in updated:
                if not overwrite:
                    continue
                from_ = f" from {updater_name}" if updater_name else ""
                print(f"Overwriting arg '{key}'{from_}: {updated[key]} -> {value}")
                updated[key] = value
            else:
                updated[key] = value

    @classmethod
    def _cli_basic(
        cls,
        # args similar to 'scrapy crawl' CLI
        crawlsetts: Annotated[list[str] | None, Opt("-s", "--set")] = None,
        crawlspargs: Annotated[list[str] | None, Opt("-a", "--arg")] = None,
        # spidercls-specific settings (specsetts)
        NOW: Annotated[str | None, Opt("-n", "--NOW")] = sett_specs["NOW"],
        DONT_STORE: Annotated[bool, Opt("--DONT-STORE")] = sett_specs[
            "DONT_STORE"
        ],  # TODO make it NO_WRITE_DB
        # spidercls-specific spiderargs (specspargs)
        url: Annotated[str | None, Opt("-u", "--url")] = sparg_specs["url"],
        save_html: Annotated[bool, Opt("-h", "--save-html")] = sparg_specs[
            "save_html"
        ],  # TODO make it a setting
        test_cli: Annotated[bool, Opt("-t", "--test-cli")] = False,
    ) -> None:
        # update _test_cli
        cls._test_cli = test_cli

        # apply 'scrapy crawl' CLI settings
        dct = {}
        for s in crawlsetts or []:
            key, value = s.split("=", 1)
            dct[key] = value
        Baze._verbose_update(cls._settings, dct, "crawlsetts")
        # apply 'scrapy crawl' CLI spiderargs
        dct = {}
        for a in crawlspargs or []:
            argname, argvalue = a.split("=", 1)
            dct[argname] = argvalue
        Baze._verbose_update(cls._spiderargs, dct, "crawlspargs")

        # apply CLI settings from class specs
        dct = {"NOW": NOW, "DONT_STORE": DONT_STORE}
        Baze._verbose_update(cls._settings, dct, "specsetts")
        # apply CLI spiderargs from class specs
        dct = {"url": url, "save_html": save_html}
        Baze._verbose_update(cls._spiderargs, dct, "specspargs")

    @classmethod
    def _cli_sub(cls) -> None:
        raise NotImplementedError

    @classmethod
    def _cli(cls) -> None:
        raise NotImplementedError

    @classmethod
    def _cli_full(cls, ctx: typer.Context, **kwargs) -> None:
        # make sure args are reset (helps in testing)
        cls._spiderargs = {}
        cls._settings = {}

        # apply yamlargs
        yamlargs = parse_yamlargs(ctx, key=cls.name, edit_ctx_values=False)
        Baze._verbose_update(kwargs, yamlargs, "yamlargs")

        # run classmethods: _cli_basic -> _cli_sub -> _cli
        for cm in [cls._cli_basic, cls._cli_sub, cls._cli]:
            kw = {k: v for k, v in kwargs.items() if k in cm.__annotations__}
            if "ctx" in cm.__annotations__:
                kw["ctx"] = ctx
            try:
                print(f"Running '{cm.__name__}' with kwargs:")
                print(kw)
                cm(**kw)
            except NotImplementedError:
                print("WARNING: not implemented")
                pass
            [kwargs.pop(k) for k in kw if k != "ctx"]
        kwargs.pop("ctx", None)

        # apply remaining settings (non class-specified that are coming from yaml)
        dct = {k: v for k, v in kwargs.items() if k.isupper()}
        Baze._verbose_update(cls._settings, dct, "unspecified yaml setting", False)
        [kwargs.pop(k) for k in dct]
        # apply remaining spiderargs (non class-specified that are coming from yaml)
        dct = {k: v for k, v in kwargs.items() if k not in cls._spiderargs}
        Baze._verbose_update(cls._spiderargs, dct, "unspecified yaml spiderarg", False)
        [kwargs.pop(k) for k in dct]

        # unused args
        if kwargs:
            print(f"WARNING: Unused args remain: {kwargs}")
            kwargs = {}

        # delete default, non-explicit settings (to be reapplied in from_crawler)
        for k, v in cls.all_sett_specs().items():
            if ctx.get_parameter_source(k) == ParameterSource.COMMANDLINE:
                continue
            if k in cls._settings and v == cls._settings[k]:
                del cls._settings[k]
        # delete default, non-explicit spiderargs (to be reapplied in __init__)
        for k, v in cls.all_sparg_specs().items():
            if ctx.get_parameter_source(k) == ParameterSource.COMMANDLINE:
                continue
            if k in cls._spiderargs and v == cls._spiderargs[k]:
                del cls._spiderargs[k]

        # run main
        cls._cli_main()

    @classmethod
    def used_args(cls):
        return {**cls._settings, **cls._spiderargs}

    @classmethod
    def _cli_main(cls) -> None:
        # print args
        print("Used args:")
        print(cls.used_args())
        # validate args
        for key, value in cls._settings.items():
            # TODO check settings types (using scrapy scrapy/utils/conf.py?)
            if key not in Settings().attributes.keys() | cls.all_sett_specs():
                raise typer.Abort(f"Unknown setting '{key}' with value '{value}'")
        for key, value in cls._spiderargs.items():
            # TODO check spiderarg types using specified arg signatures
            if key not in cls.all_sparg_specs():
                raise typer.Abort(f"Unknown spiderarg '{key}' with value '{value}'")
        # run main or exit if test-cli flag is enabled
        if cls._test_cli:
            print("End of cli-test")
            raise typer.Exit()
        print("Running main")
        cls.main()

    @classmethod
    def main(cls) -> None:
        # create process with cli and project settings
        project_settings = get_project_settings()
        for key, value in cls._settings.items():
            project_settings.set(key, value, priority="cmdline")
        process = CrawlerProcess(settings=project_settings)
        # crawl with spiderargs
        process.crawl(cls, **cls._spiderargs)
        process.start()

    @classmethod
    def cli_app(cls) -> typer.Typer:
        """Permits conveniently running typer.testing.CliRunner().invoke in spider test
        modules by providing its first argument, the typer app object."""
        app = typer.Typer()
        # define docstr here rather than in func doc to protect it from docformatter
        helpstr = f"""
        CLI interface to {cls.__name__}.main (buffed version of 'scrapy crawl' CLI)\n
        Supports the following:\n
        1) Reading args/settings from a yaml (config) file\n
        2) Using --arg/-a and --set/-s options similarly to 'scrapy crawl'\n
        3) Spider-specific arguments that offer help, validation, and default values\n.
        """

        # create a wrapper function so that we can give it a __signature__
        # (not possible to set __signature__ on a classmethod)
        def cli_full(**kwargs):
            cls._cli_full(**kwargs)

        def dummy_func_with_ctx(ctx: typer.Context):
            pass

        fullsig_dict = {}
        for cm in [cls._cli_basic, cls._cli_sub, cls._cli]:
            sig = inspect.signature(cm)
            fullsig_dict.update(sig.parameters)
        ctx_sig = {"ctx": inspect.signature(dummy_func_with_ctx).parameters["ctx"]}
        full_signature = inspect.Signature([*{**ctx_sig, **fullsig_dict}.values()])
        cli_full.__signature__ = full_signature  # type: ignore
        annotations = {
            name: param.annotation for name, param in full_signature.parameters.items()
        }
        cli_full.__annotations__ = annotations
        app.command(help=helpstr)(cli_full)
        cls.app = app
        return app

    @classmethod
    def run_cli(cls) -> None:
        """Permits convenienty runnning cls.main from spider subclass modules via
        Subclass.cli(); no the need to import typer and use typer.run(subclass.main)."""
        app = cls.cli_app()
        app()

    @classmethod
    def get_samples(cls):
        return []

    @classmethod
    def itemcls(cls):
        raise NotImplementedError


class Meta(type):
    """Metaclass enforcing assumptions for the subclasses of Baze's subclasses."""

    def __new__(cls, name, bases, dct):
        c = super().__new__(cls, name, bases, dct)
        if not c.__base__:
            raise RuntimeError(f"No baseclass for {c}")

        if len(bases) > 1:
            raise RuntimeError(
                "Multiple inheritance is not supported for Baze subclasses"
            )

        c_is_baseclass = c.__base__ is Baze

        # check spidertype
        spidertype = name[-3:]  # the last 3 chars are the spider type: src/lst
        if c_is_baseclass:
            if spidertype not in ["Lst", "Src"]:
                raise RuntimeError(
                    "Baze's direct subclasses' names should end in Lst/Src"
                )
        else:
            expected_spidertype = c.__base__.__name__[-3:]
            if spidertype != expected_spidertype:
                raise RuntimeError(
                    "{} subclasses' names should end in {}, unlike {}".format(
                        c.__base__.__name__, expected_spidertype, name
                    )
                )

        # read info from yml for non-baseclasses
        if c_is_baseclass:
            return c

        if "info_file" not in c.__dict__:
            info_file = "info.yml"
        else:
            info_file = c.__dict__["info_file"]

        if not info_file:
            return c
        codename = name[:-3].lower()
        info = yaml_get(info_file, [codename])
        if not info:
            print(f"Could not find info for shop '{codename}'")
        spider_info = info.get(spidertype.lower(), {})
        # set custom classvars: shop name, html_fields
        setattr(c, "shop", info.get("name", codename))
        setattr(c, "fields", spider_info.get("fields", {}))
        # set scrapy classvars: spider name, start_urls, allowed_domains
        setattr(c, "name", codename + "_" + spidertype.lower())
        setattr(c, "start_urls", spider_info.get("start_urls", []))
        setattr(c, "allowed_domains", spider_info.get("allowed_domains", []))

        return c


class BazeLst(Baze, metaclass=Meta):
    name = "lst"

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider.settings["ITEM_PIPELINES"][ListingStorer] = 500
        # make sure start_urls is not empty
        # don't enforce this in spider.__init__ to allow for testing flexibility
        if not spider.start_urls:
            raise ValueError(
                "self.start_urls is empty. Please provide the 'url' spiderarg"
            )
        return spider


class BazeSrc(Baze, metaclass=Meta):
    name = "src"
    sparg_specs = {
        "include1": None,
        "include2": None,
        "include3": None,
        "exclude1": None,
        "exclude2": None,
        "exclude3": None,
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider.settings["ITEM_PIPELINES"][SourceStorer] = 500
        return spider

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)
        # include/exclude category args
        include_all = list(LATIN_ALPHABET)  # all cats have letters
        self.include1 = getattr(self, "include1", include_all)
        self.include2 = getattr(self, "include2", include_all)
        self.include3 = getattr(self, "include3", include_all)
        exclude_none = [" "]  # no subcats have spaces
        self.exclude1 = getattr(self, "exclude1", exclude_none)
        self.exclude2 = getattr(self, "exclude2", exclude_none)
        self.exclude3 = getattr(self, "exclude3", exclude_none)

    def to_be_skipped(self, category: str, level: int):
        if level not in (1, 2, 3):
            raise ValueError(f"Supported values for level are 1, 2, 3 (not {level})")
        included = getattr(self, f"include{level}")
        excluded = getattr(self, f"exclude{level}")
        is_included = any(map(lambda inc: inc in category, included))
        is_excluded = any(map(lambda exc: exc in category, excluded))
        skip = not is_included or is_excluded
        if skip:
            self.logger.info(f"To be skipped: category={category} level={level}")
        return skip

    @classmethod
    def _cli_sub(  # type: ignore
        cls,
        include1: Annotated[
            list[str] | None, typer.Option("-i1", "--inc1")
        ] = sparg_specs["include1"],
        include2: Annotated[
            list[str] | None, typer.Option("-i2", "--inc2")
        ] = sparg_specs["include2"],
        include3: Annotated[
            list[str] | None, typer.Option("-i3", "--inc3")
        ] = sparg_specs["include3"],
        exclude1: Annotated[
            list[str] | None, typer.Option("-x1", "--exc1")
        ] = sparg_specs["exclude1"],
        exclude2: Annotated[
            list[str] | None, typer.Option("-x2", "--exc2")
        ] = sparg_specs["exclude2"],
        exclude3: Annotated[
            list[str] | None, typer.Option("-x3", "--exc3")
        ] = sparg_specs["exclude3"],
    ) -> None:
        # apply class-specified CLI spiderargs
        dct = {
            "include1": include1,
            "include2": include2,
            "include3": include3,
            "exclude1": exclude1,
            "exclude2": exclude2,
            "exclude3": exclude3,
        }
        Baze._verbose_update(cls._spiderargs, dct, "specspargs")


if __name__ == "__main__":

    class ShopSrc(BazeSrc):
        # custom_settings={} # TODO test custom settings

        @classmethod
        def _cli(cls) -> None:
            print("shopsrc cli")

    ShopSrc.run_cli()
