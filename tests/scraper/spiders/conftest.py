from pathlib import Path
from typing import Any

import pytest

from data_mastor.cliutils import get_yamldict_key
from data_mastor.scraper.spiders import USED_ARGS_FILENAME, Baze, timestamp


class ResultGatherer:
    items_scraped = []
    spider_settings = {}

    def open_spider(self, spider):
        self.spider_settings = spider.settings

    def process_item(self, item, spider):
        self.items_scraped.append(item)
        return item


@pytest.fixture
def gatherercls():
    ResultGatherer.items_scraped = []
    return ResultGatherer


# this has to be defined per test-case/module
@pytest.fixture
def spidercls():
    """To be defined per testmodule."""
    raise NotImplementedError


# define this for each testcase if not directly defining
@pytest.fixture
def testcase_dir() -> str | Path:
    """To be defined per testcase."""
    raise NotImplementedError


# this has to be defined for each testcase
@pytest.fixture
def yamlconf(spidercls: type[Baze], testcase_dir: str | Path) -> dict[str, Any]:
    yamlpath = Path("tests/data") / spidercls.name / testcase_dir / USED_ARGS_FILENAME
    yamlconf = get_yamldict_key(yamlpath, spidercls.name, doraise=True)
    print(f"yamlconf from {yamlpath}:")
    print(f"{yamlconf}")
    return yamlconf


# testing-specific configuration
TESTING_SPIDERARGS = {"save_html": False}
TESTING_SETTINGS = {
    # artifacts
    "LOG_FILE": None,
    "FEEDS": {},
    # pipelines
    "ITEM_PIPELINES": {ResultGatherer: 1},
    "DONT_STORE": True,
    # misc
    "DOWNLOAD_DELAY": 0,
    "DEFAULT_REQUEST_HEADERS": {"User-Agent": "testagent"},
    "USER_AGENT": "test-agent",
}


@pytest.fixture(autouse=True)
def configure_spidercls(spidercls: Baze, yamlconf, request):
    """Configure spidercls for testing.

    To apply a testmodule-specific configuration, either override this fixture in the
    testmodule and/or modify the spidercls fixture itself.
    """
    # get spidercls config (can be defined at testcase level, should precede others)
    _settings = spidercls._settings
    _spiderargs = spidercls._spiderargs

    # get yaml config
    yaml_settings = {k: v for k, v in yamlconf.items() if k.isupper()}
    yaml_spiderargs = {k: v for k, v in yamlconf.items() if not k.isupper()}

    # get testing config and update with out_dir
    testname = request.node.name.replace("[", "_").replace("]", "")
    assert isinstance(testname, str)
    out_dir = Path("tests/out") / (timestamp + "_" + testname + "_" + spidercls.name)
    TESTING_SETTINGS["OUT_DIR"] = out_dir

    # apply config
    spidercls._settings = {**yaml_settings, **TESTING_SETTINGS, **_settings}
    spidercls._spiderargs = {**yaml_spiderargs, **TESTING_SPIDERARGS, **_spiderargs}
    print(f"Configured {spidercls}")
    yield
    spidercls._settings = {}
    spidercls._spiderargs = {}
    msg = "Test failed!" if request.node.rep_call.failed else "Test passed!"
    print(msg)
    print(f"Out dir: {out_dir}")
