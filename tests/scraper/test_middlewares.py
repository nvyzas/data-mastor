import os
import tempfile
from pathlib import Path
from types import ModuleType
from typing import Callable
from unittest.mock import MagicMock

import pytest
from itemadapter import ItemAdapter
from pytest import FixtureRequest
from pytest_mock import MockerFixture
from scrapy import Request, Spider
from scrapy.http import HtmlResponse
from scrapy.http.headers import Headers
from scrapy.settings import Settings

from data_mastor.scraper import middlewares
from data_mastor.scraper.middlewares import (
    ENVVAR_ALLOWED_INTERFACE,
    ENVVAR_NO_LEAK_TEST,
    ENVVAR_NO_UA_TEST,
    ENVVAR_PROXY_IP,
    NO_LEAK_TEST_WARNING,
    NO_UA_CHECK_WARNING,
    PrivacyCheckerDlMw,
    ResponseSaverSpMw,
    _interface_ip,
    _interface_is_up,
    _is_leaking,
)
from data_mastor.scraper.utils import abort


def _patch_path(obj: Callable, at_module: ModuleType | None = None) -> str:
    path = obj.__module__ if at_module is None else at_module.__name__
    return path + "." + obj.__name__


class AbortException(Exception):
    pass


@pytest.fixture
def mock_abort(mocker: MockerFixture) -> MagicMock:
    mock = mocker.patch(_patch_path(abort, middlewares))
    mock.side_effect = AbortException("Test abort called")
    return mock


class TestPrivacyCheckerDlMw:
    @pytest.fixture
    def middleware(self) -> PrivacyCheckerDlMw:
        """Create a middleware instance for testing."""
        return PrivacyCheckerDlMw()

    @pytest.fixture(params=["uatestY", "uatestN"])
    def env_no_ua_test(self, request: FixtureRequest) -> str:
        os.environ[ENVVAR_NO_UA_TEST] = "" if request.param == "uatestY" else "true"
        return os.environ[ENVVAR_NO_UA_TEST]

    @pytest.fixture(params=["proxyY", "proxyN"])
    def env_proxy_ip(self, request: FixtureRequest) -> str:
        os.environ[ENVVAR_PROXY_IP] = "1.2.3.4" if request.param == "proxyY" else ""
        return os.environ[ENVVAR_PROXY_IP]

    @pytest.fixture(params=["ifaceY", "ifaceN"])
    def env_allowed_interface(self, request: FixtureRequest) -> str:
        os.environ[ENVVAR_ALLOWED_INTERFACE] = (
            "allowed-interface-name" if request.param == "ifaceY" else ""
        )
        return os.environ[ENVVAR_ALLOWED_INTERFACE]

    @pytest.fixture(params=["leaktestY", "leaktestN"])
    def env_no_leak_test(self, request: FixtureRequest) -> str:
        os.environ[ENVVAR_NO_LEAK_TEST] = "" if request.param == "leaktestY" else "true"
        return os.environ[ENVVAR_NO_LEAK_TEST]

    @pytest.fixture
    def mock_spider(self, mocker: MockerFixture) -> MagicMock:
        spider = mocker.Mock()
        spider.name = "testina"
        spider.logger = mocker.Mock()
        return spider

    @pytest.fixture(params=["isupY", "isupN"])
    def mock_interface_is_up(
        self, mocker: MockerFixture, request: FixtureRequest
    ) -> MagicMock:
        mock = mocker.patch(_patch_path(_interface_is_up))
        mock.return_value = True if request.param == "isupY" else False
        return mock

    @pytest.fixture(params=["ifaceipY", "ifaceipN"])
    def mock_interface_ip(
        self, mocker: MockerFixture, request: FixtureRequest
    ) -> MagicMock:
        mock = mocker.patch(_patch_path(_interface_ip))
        mock.return_value = "5.6.7.8" if request.param == "ifaceipY" else ""
        return mock

    @pytest.fixture(params=["leaksY", "leaksN"])
    def mock_is_leaking(
        self, mocker: MockerFixture, request: FixtureRequest
    ) -> MagicMock:
        mock = mocker.patch(_patch_path(_is_leaking))
        mock.return_value = True if request.param == "leaksY" else False
        return mock

    def test_spider_opened(
        self,
        middleware: PrivacyCheckerDlMw,
        env_proxy_ip: str,
        env_allowed_interface: str,
        env_no_ua_test: str,
        env_no_leak_test: str,
        mock_spider: MagicMock,
        mock_is_leaking: MagicMock,
        mock_interface_is_up: MagicMock,
        mock_interface_ip: MagicMock,
        mock_abort: MagicMock,
    ) -> None:
        """Test spider_opened method with various environment configurations."""
        try:
            middleware.spider_opened(mock_spider)
        except AbortException:
            print("Abort was called")

        mock_warning: MagicMock = mock_spider.logger.warning

        # user-agent check warning
        if env_no_ua_test:
            assert mock_warning.call_args_list[0].args[0] == NO_UA_CHECK_WARNING

        # allowed interface check
        if env_allowed_interface and not env_proxy_ip:
            if not mock_interface_is_up() or not mock_interface_ip():
                mock_abort.assert_called_once()
                return

        # leaktest
        if env_no_leak_test:
            assert mock_warning.call_args_list[-1].args[0] == NO_LEAK_TEST_WARNING
        else:
            if mock_is_leaking():
                mock_abort.assert_called_once()

    def test_process_request(
        self,
        mock_spider: MagicMock,
        middleware: PrivacyCheckerDlMw,
        mock_abort: MagicMock,
    ) -> None:
        # create dummy request
        request = Request("http://example.com")

        # Test 1  - User-Agent validation
        middleware._check_ua = True
        middleware._proxy_ip = ""  #
        middleware._interface_ip = ""

        # Test 1a - good User-Agent
        request.headers = Headers({"User-Agent": "good-user-agent"})
        result = middleware.process_request(request, mock_spider)
        assert result is None
        assert not mock_abort.called

        # Test 1b - missing User-Agent
        request.headers = Headers({})
        with pytest.raises(AbortException):
            middleware.process_request(request, mock_spider)
        mock_abort.assert_called_once()
        mock_abort.reset_mock()

        # Test 1c - bad User-Agent
        request.headers = Headers({"User-Agent": "bot"})
        with pytest.raises(AbortException):
            middleware.process_request(request, mock_spider)
        mock_abort.assert_called_once()
        mock_abort.reset_mock()

        # Test 1d - check disabled (bad User-Agent)
        middleware._check_ua = False
        result = middleware.process_request(request, mock_spider)
        assert result is None
        assert not mock_abort.called

        # Test 2 - proxy meta attribute
        request._meta = {}
        middleware._proxy_ip = "http://proxy:8080"
        result = middleware.process_request(request, mock_spider)
        assert result is None
        assert request.meta["proxy"] == "http://proxy:8080"
        assert "bindaddress" not in request.meta

        # Test 3 - bindaddress meta attribute
        request._meta = {}
        middleware._proxy_ip = ""
        middleware._interface_ip = "192.168.1.100"
        result = middleware.process_request(request, mock_spider)
        assert result is None
        assert request.meta["bindaddress"] == "192.168.1.100"
        assert "proxy" not in request.meta


class TestResponseSaverSPMW:
    @pytest.fixture
    def mw(self) -> ResponseSaverSpMw:
        """Create a ResponseSaverDLMW instance."""
        return ResponseSaverSpMw()

    @pytest.fixture
    def tmpdir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            assert path.exists()
            print(f"Created {path}")
            yield path
            assert path.exists()
        assert not path.exists()

    def test_process_spider_output(
        self, mw: ResponseSaverSpMw, tmpdir: Path, mock_abort: MagicMock
    ) -> None:
        # setup
        req1 = Request(url="http://example.com/page1.html")
        response1 = HtmlResponse(
            url=req1.url,
            request=req1,
            body=b"<html><body>Test content</body></html>",
        )
        spider = Spider(name="testina")
        spider.settings = Settings()
        # Abort if OUT_DIR is not set
        with pytest.raises(AbortException):
            _ = list(mw.process_spider_output(response1, [], spider))
        mock_abort.assert_called_once()
        # set OUT_DIR
        spider.settings["OUT_DIR"] = str(tmpdir)
        # Don't save
        spider.settings["SAVE_HTML"] = False
        _ = list(mw.process_spider_output(response1, [], spider))
        files = list(tmpdir.glob("*.html"))
        assert len(files) == 0
        # Save html
        spider.settings["SAVE_HTML"] = True
        _ = list(mw.process_spider_output(response1, [], spider))
        files = list(tmpdir.glob("*.html"))
        assert len(files) == 1
        assert files[0].read_bytes() == response1.body
        # Disable saving
        spider.settings["SAVE_HTML"] = False
        # Don't localize follow-up request url
        item = ItemAdapter({"key": "value"})  # a scraped item
        req2 = Request(url="http://example.com/page2.html")  # a follow-up request
        res = list(mw.process_spider_output(response1, [item, req2], spider))
        assert res[0] == item
        assert res[1] == req2
        # Localize follow-up request url
        request_local = Request(url=f"file://{tmpdir.absolute()}/page1.html")
        response_local = HtmlResponse(
            request=request_local,
            url=request_local.url,
            body=b"<html><body>Test content local</body></html>",
        )
        res_local = list(mw.process_spider_output(response_local, [item, req2], spider))
        assert res_local[0] == item
        assert isinstance(res_local[1], Request)
        assert res_local[1].url == mw._generate_html_url(tmpdir, res_local[1].url)


if __name__ == "__main__":
    pytest.main([__file__])
