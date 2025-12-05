from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from data_mastor.scraper.spiders import Baze, BazeSrc
from data_mastor.utils import assert_result_exit_code


@pytest.fixture
def mock_main(mocker: MockerFixture) -> MagicMock:
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
        ["test_cli_flag", "main_was_called"], [(True, False), (False, True)]
    )
    def test_testcli_flag(
        self,
        spidercls: type[Baze] | type[BazeSrc] | type[ShopSrc],
        test_cli_flag: bool,
        main_was_called: bool,
        mock_main: MagicMock,
    ) -> None:
        args = ["--test-cli"] if test_cli_flag else []
        result = CliRunner().invoke(spidercls.cli_app(), args)
        assert_result_exit_code(result)
        assert mock_main.called is main_was_called

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
    # src args
    cmd1 = ["-i1", "bo", "--inc1", "so", "-x1", "ko", "--exc3", "ro"]
    cmd01 = cmd0 + cmd1
    spargs1 = {"include1": ["bo", "so"], "exclude1": ["ko"], "exclude3": ["ro"]}
    spargs01 = {**spargs0, **spargs1}

    # test-case data
    cases = {
        # no args
        "noargs_baze": (Baze, [], {}, {}),
        "noargs_src": (BazeSrc, [], {}, {}),
        "noargs_test": (ShopSrc, [], {}, {}),
        # baze args
        "bazeargs_cli_baze": (Baze, cmd0, spargs0, setts0),
        "bazeargs_cli_src": (BazeSrc, cmd0, spargs0, setts0),
        "bazeargs_cli_test": (ShopSrc, cmd0, spargs0, setts0),
        # src args
        "srcargs_cli_src": (BazeSrc, cmd1, spargs1, {}),
        "srcargs_cli_test": (ShopSrc, cmd1, spargs1, {}),
        # baze+src args
        "srcbazeargs_cli_src": (BazeSrc, cmd01, spargs01, setts0),
        "srcbazeargs_cli_test": (ShopSrc, cmd01, spargs01, setts0),
    }

    @pytest.mark.parametrize(
        ["spidercls", "cliargs", "expected_spiderargs", "expected_settings"],
        cases.values(),
        ids=cases.keys(),
    )
    def test_args(
        self,
        mock_main: MagicMock,
        spidercls: Baze,
        cliargs,
        expected_spiderargs,
        expected_settings,
    ) -> None:
        spidercls.reset()
        result = CliRunner().invoke(spidercls.cli_app(), cliargs)
        assert_result_exit_code(result)
        mock_main.assert_called_once()
        assert spidercls._spiderargs == expected_spiderargs
        assert spidercls._settings == expected_settings

    @pytest.mark.parametrize("spidercls", [Baze, BazeSrc, ShopSrc])
    @pytest.mark.parametrize(
        "args", [["-a", "invalidspiderarg=1"], ["-s", "invalidsetting=2"]]
    )
    def test_invalid_arg(
        self,
        spidercls: type[Baze] | type[BazeSrc] | type[ShopSrc],
        args: list[str],
        mock_main: MagicMock,
        mocker: MockerFixture,
    ) -> None:
        mock_abort = mocker.patch("typer.Abort")
        result = CliRunner().invoke(spidercls.cli_app(), args)
        print(result.stdout)
        mock_abort.assert_called_once()
        mock_main.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__])
