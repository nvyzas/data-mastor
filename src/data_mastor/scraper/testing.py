"""A collection of fixtures for testing spiders doing actual crawling sessions."""

import logging
import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session

from data_mastor.cliutils import read_yaml
from data_mastor.dbman import get_engine
from data_mastor.scraper.models import Base
from data_mastor.scraper.spiders import USED_ARGS_FILENAME, Baze, timestamp
from data_mastor.scraper.utils import configure_scrapy_logging_levels
from data_mastor.utils import nested_dict_get

# dummy
importable_fixture = "dummy"


# DATABASE
TEST_DB_URL = "sqlite:///:memory:"


@pytest.fixture(scope="session", autouse=True)
def setup_db_url() -> None:
    os.environ["DB_URL"] = TEST_DB_URL


@pytest.fixture(scope="session")
def engine(setup_db_url):
    kwargs = {"connect_args": {"check_same_thread": False}}
    engine = get_engine(**kwargs)
    print(f"Engine URL: {engine.url}")
    assert str(engine.url) == TEST_DB_URL
    Base.metadata.create_all(engine)
    yield engine


@pytest.fixture(scope="session")
def sessmkr(engine: Engine):
    yield sessionmaker(bind=engine)


@pytest.fixture(scope="function")
def sess(sessmkr: sessionmaker[Session]):
    with sessmkr() as s:
        yield s


@pytest.fixture
def reset_db(sessmkr: sessionmaker[Session]):
    with sessmkr() as session:
        Base.metadata.drop_all(session.get_bind())
        Base.metadata.create_all(session.get_bind())


@pytest.fixture
def entities() -> list[Base]:
    return []


@pytest.fixture
def fill_with_entities(
    reset_db: None, sessmkr: sessionmaker[Session], entities: Iterable[Base]
) -> None:
    with sessmkr() as session:
        session.add_all(entities)


# SPIDER


@pytest.fixture
def spidercls() -> type[Baze]:
    """To be defined per testcase."""
    raise NotImplementedError


@pytest.fixture
def testcase_dir() -> str | Path:
    """To be defined per testcase."""
    raise NotImplementedError


# this has to be defined for each testcase
@pytest.fixture
def yamlargs(spidercls: type[Baze], testcase_dir: str | Path) -> dict[str, Any]:
    logging.debug("Running yamlargs fixture")
    yamlpath = Path("tests/data") / spidercls.name / testcase_dir / USED_ARGS_FILENAME
    yamlcontents = read_yaml(yamlpath)
    keys, yamlargs = nested_dict_get(yamlcontents, raise_on_error=True)
    print(f"Yamlargs from {yamlpath} under {keys}:")
    print(f"{yamlargs}")
    return yamlargs


class ResultGatherer:
    items_scraped: list[Any] = []
    spider_settings: dict[str, Any] = {}

    def open_spider(self, spider):
        self.spider_settings = spider.settings

    def process_item(self, item, spider):
        self.items_scraped.append(item)
        return item


@pytest.fixture(scope="function")  # reset items_scraped after each test
def gatherercls() -> type[ResultGatherer]:
    logging.debug("Running gatherercls fixture")
    return ResultGatherer


@pytest.fixture
def configured_spidercls[T: Baze](
    spidercls: type[T],
    yamlargs: dict[str, Any],
    gatherercls: type[ResultGatherer],
    request: pytest.FixtureRequest,
):
    """Configure spidercls for testing.

    To apply a testmodule-specific configuration, either override this fixture in the
    testmodule and/or modify the spidercls fixture itself.
    """
    logging.debug("Running configure_spidercls fixture")
    # configure scrapy logging levels
    configure_scrapy_logging_levels()

    # get spidercls config (can be defined at testcase level, should precede others)
    _settings = spidercls._settings
    _spiderargs = spidercls._spiderargs

    # get yaml config
    yaml_settings = {k: v for k, v in yamlargs.items() if k.isupper()}
    yaml_spiderargs = {k: v for k, v in yamlargs.items() if not k.isupper()}

    # get testing config and update with out_dir
    # testing-specific configuration
    TESTING_SPIDERARGS = {"save_html": False}
    TESTING_SETTINGS = {
        # artifacts: make sure no test out is created
        "LOG_LEVEL": "INFO",
        "LOG_FILE": None,
        "FEEDS": {},
        # pipelines: make sure all scraped items are collected and persisted (in memory)
        "ITEM_PIPELINES": {gatherercls: 1},
        "DONT_STORE": False,
        # misc
        "DOWNLOAD_DELAY": 0,
        "USER_AGENT": "testagent",
    }
    # configure the output dir for artifacts
    testname = request.node.name.replace("[", "_").replace("]", "")
    assert isinstance(testname, str)
    out_dir = Path("tests/out") / (timestamp + "_" + testname + "_" + spidercls.name)
    TESTING_SETTINGS["OUT_DIR"] = str(out_dir.absolute())

    # apply config
    spidercls._settings = {**yaml_settings, **TESTING_SETTINGS, **_settings}
    spidercls._spiderargs = {**yaml_spiderargs, **TESTING_SPIDERARGS, **_spiderargs}
    yield spidercls
    spidercls._settings = {}
    spidercls._spiderargs = {}
    try:
        request.node.rep_call.failed
    except AttributeError:
        print("Test passed!")
        if out_dir.is_file():
            print(f"Out dir: {out_dir}")
    else:
        print("Test failed!")
