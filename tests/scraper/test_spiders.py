import traceback
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from data_mastor.scraper.spiders import Baze, BazeSrc


# override conftest fixtures
@pytest.fixture(scope="module")
def spidercls():
    pass


@pytest.fixture
def configure_spidercls():
    pass


@pytest.fixture
def check_spidercls_testing_configuration():
    pass


# define helper for error detection
def _assert_no_error(result):
    if result.stderr or result.exit_code != 0 or result.exception:
        print("Invoked-command failed")
        print(f"stderr: {result.stderr}")
        print(f"exit_code: {result.exit_code}")
        print(f"exception: {result.exception}")
        print(f"exc_info: {result.exc_info}")
        e = result.exception
        tb = "".join(traceback.format_exception(type(e), value=e, tb=e.__traceback__))
        print(tb)
        assert False


@pytest.fixture
def yamlargs():
    return {}


@pytest.fixture
def yamlconfmock(mocker: MockerFixture, yamlargs):
    mock: MagicMock = mocker.patch("data_mastor.cliutils.yaml_get")
    mock.return_value = yamlargs
    yield mock
    mock.assert_called_once()


@pytest.fixture
def mainmock(mocker: MockerFixture) -> MagicMock:
    mock = mocker.patch.object(Baze, Baze.main.__name__, spec=Baze.main, __name__="abc")
    return mock


class ShopSrc(BazeSrc):
    # custom_settings={} # DO test priority rules for custom settings

    __test__ = False

    @classmethod
    def _cli(cls) -> None:
        print("shopsrc cli")


class TestCLI:
    @pytest.fixture(scope="class", autouse=True)
    def turnoff_forked(self, request: pytest.FixtureRequest):
        request.config.option.forked = False

    @pytest.mark.parametrize("spidercls", [Baze, BazeSrc, ShopSrc])
    @pytest.mark.parametrize(
        "test_cli_flag,main_was_called", [(True, False), (False, True)]
    )
    def test_testcli_flag(
        self,
        spidercls: type[Baze] | type[BazeSrc] | type[ShopSrc],
        test_cli_flag: bool,
        main_was_called: bool,
        mainmock: MagicMock,
    ) -> None:
        args = ["--test-cli"] if test_cli_flag else []
        result = CliRunner().invoke(spidercls.cli_app(), args)
        _assert_no_error(result)
        assert mainmock.called is main_was_called

    # baze args
    crawlspargs_cli = [
        "-a",
        "url=crawl/url",  # specified sparg (to be overriden)
        "-a",
        "save_html=False",  # specified  sparg (to be overriden)
    ]
    crawlsetts_cli = [
        "-s",
        "NOW=123",  # specified setting (to be overriden)
        "-s",
        "DOWNLOAD_DELAY=3",  # unspecified setting
    ]
    specspargs_cli = ["--url", "cmdline/url", "--save-html"]
    specsetts_cli = ["--NOW", "456", "--DONT-STORE"]
    cmd0 = crawlspargs_cli + crawlsetts_cli + specspargs_cli + specsetts_cli
    spargs0 = {"url": "cmdline/url", "save_html": True}
    setts0 = {"NOW": "456", "DOWNLOAD_DELAY": "3", "DONT_STORE": True}
    # try yaml args that are identical to CLI args
    yml0 = spargs0 | setts0
    # try yaml args that are different than CLI args
    ymlspargs0B = {
        "url": "yaml/url"  # specsparg different value (to be overriden)
    }
    ymlsetts0B = {
        "DONT_STORE": False,  # specsett different value (to be overriden)
        "DOWNLOAD_DELAY": "4",  # crawlsett different value (to be overriden)
        "DOWNLOADER_MIDDLEWARES": {"mw1": 950, "mw2": 25},  # non-spec, dict setting
    }
    yml0B = ymlspargs0B | ymlsetts0B
    expspargs0B = ymlspargs0B | spargs0
    expsetts0B = ymlsetts0B | setts0
    # src args
    cmd1 = ["-i1", "bo", "--inc1", "so", "-x1", "ko", "--exc3", "ro"]
    cmd01 = cmd0 + cmd1
    spargs1 = {"include1": ["bo", "so"], "exclude1": ["ko"], "exclude3": ["ro"]}
    spargs01 = {**spargs0, **spargs1}
    # try yaml args that are identical to CLI args
    ymlspargs1 = {**spargs1}
    ymlspargs01 = {**spargs01}
    yml1 = {**ymlspargs1}
    yml01 = {**ymlspargs01, **setts0}
    # try yaml args that are different than CLI args
    ymlspargs1B = {"exclude1": ["xo"], "include3": ["zo"]}
    ymlspargs01B = {**ymlspargs0B, **ymlspargs1B}
    yml01B = {**ymlspargs01B, **setts0}
    expspargs01B = ymlspargs01B | spargs01
    expsetts01B = {**setts0}

    # test-case data
    cases = {
        # no args
        "noargs_baze": (Baze, [], {}, {}, {}),
        "noargs_src": (BazeSrc, [], {}, {}, {}),
        "noargs_test": (ShopSrc, [], {}, {}, {}),
        # baze args
        "bazeargs_cli_baze": (Baze, cmd0, {}, spargs0, setts0),
        "bazeargs_cli_src": (BazeSrc, cmd0, {}, spargs0, setts0),
        "bazeargs_cli_test": (ShopSrc, cmd0, {}, spargs0, setts0),
        "bazeargs_yml_baze": (Baze, [], yml0, spargs0, setts0),
        "bazeargs_yml_src": (BazeSrc, [], yml0, spargs0, setts0),
        "bazeargs_yml_test": (ShopSrc, [], yml0, spargs0, setts0),
        "bazeargs_cliyml_baze": (Baze, cmd0, yml0, spargs0, setts0),
        "bazeargs_cliyml_src": (BazeSrc, cmd0, yml0, spargs0, setts0),
        "bazeargs_cliyml_test": (ShopSrc, cmd0, yml0, spargs0, setts0),
        "bazeargs_cliymlB_baze": (Baze, cmd0, yml0B, expspargs0B, expsetts0B),
        "bazeargs_cliymlB_src": (BazeSrc, cmd0, yml0B, expspargs0B, expsetts0B),
        "bazeargs_cliymlB_test": (ShopSrc, cmd0, yml0B, expspargs0B, expsetts0B),
        # src args
        "srcargs_cli_src": (BazeSrc, cmd1, {}, spargs1, {}),
        "srcargs_cli_test": (ShopSrc, cmd1, {}, spargs1, {}),
        "srcargs_yml_src": (BazeSrc, [], yml1, spargs1, {}),
        "srcargs_yml_test": (ShopSrc, [], yml1, spargs1, {}),
        "srcargs_cliyml_src": (BazeSrc, cmd1, yml1, spargs1, {}),
        "srcargs_cliyml_test": (ShopSrc, cmd1, yml1, spargs1, {}),
        # baze+src args
        "srcbazeargs_cli_src": (BazeSrc, cmd01, {}, spargs01, setts0),
        "srcbazeargs_cli_test": (ShopSrc, cmd01, {}, spargs01, setts0),
        "srcbazeargs_yml_src": (BazeSrc, [], yml01, spargs01, setts0),
        "srcbazeargs_yml_test": (ShopSrc, [], yml01, spargs01, setts0),
        "srcbazeargs_cliyml_src": (BazeSrc, cmd01, yml01, spargs01, setts0),
        "srcbazeargs_cliyml_test": (ShopSrc, cmd01, yml01, spargs01, setts0),
        "srcbazeargs_cliymlB_src": (BazeSrc, cmd01, yml01B, expspargs01B, expsetts01B),
        "srcbazeargs_cliymlB_test": (ShopSrc, cmd01, yml01B, expspargs01B, expsetts01B),
    }

    @pytest.mark.parametrize(
        "testcls,testargs,yamlargs,expected_spiderargs,expected_settings",
        cases.values(),
        ids=cases.keys(),
    )
    def test_args(
        self,
        yamlconfmock: MagicMock,
        mainmock: MagicMock,
        testcls: Baze,
        testargs,
        yamlargs,
        expected_spiderargs,
        expected_settings,
    ) -> None:
        result = CliRunner().invoke(testcls.cli_app(), testargs)
        print(result.stdout)
        _assert_no_error(result)
        mainmock.assert_called_once()
        assert testcls._spiderargs == expected_spiderargs
        assert testcls._settings == expected_settings

    @pytest.mark.parametrize("spidercls", [Baze, BazeSrc, ShopSrc])
    @pytest.mark.parametrize(
        "args", [["-a", "invalidspiderarg=1"], ["-s", "invalidsetting=2"]]
    )
    def test_invalid_arg(
        self, spidercls, args, mainmock: MagicMock, mocker: MockerFixture
    ):
        mock = mocker.patch("typer.Abort")
        result = CliRunner().invoke(spidercls.cli_app(), args)
        print(result.stdout)
        mock.assert_called_once()
        mainmock.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__])
