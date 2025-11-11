import os
from types import ModuleType
from typing import Callable
from unittest.mock import MagicMock

import pytest
from pytest import FixtureRequest
from pytest_mock import MockerFixture
from scrapy import Request, Spider
from scrapy.exceptions import CloseSpider
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

    @pytest.fixture
    def mock_abort(self, mocker: MockerFixture) -> MagicMock:
        mock = mocker.patch(_patch_path(abort, middlewares))
        mock.side_effect = AbortException("Test abort called")
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
    def middleware(self):
        """Create a ResponseSaverDLMW instance."""
        return ResponseSaverSpMw()

    @pytest.fixture
    def scrapy_request(self):
        """Create a mock request."""
        return Request(url="http://example.com/page.html")

    @pytest.fixture
    def response(self, scrapy_request):
        """Create a mock HTML response."""
        return HtmlResponse(
            url="http://example.com/page.html",
            body=b"<html><body>Test content</body></html>",
            request=scrapy_request,
        )

    @pytest.fixture
    def local_response(self):
        """Create a mock local file response."""
        req = Request(url="file:///tmp/test.html")
        return HtmlResponse(
            url="file:///tmp/test.html",
            body=b"<html><body>Local content</body></html>",
            request=req,
        )

    def test_requires_out_dir_when_save_html_enabled(
        self, middleware, response, mocker
    ):
        """Test that middleware aborts if OUT_DIR is not set when SAVE_HTML is True."""
        spider = mocker.Mock(spec=Spider)
        spider.name = "test_spider"
        spider.settings = Settings({"SAVE_HTML": True})
        spider.settings.getbool = mocker.Mock(return_value=True)
        spider.logger = mocker.Mock()
        spider.crawler = mocker.Mock()
        spider.start_urls = []

        # Mock abort to raise CloseSpider
        mock_abort = mocker.patch(
            "data_mastor.scraper.middlewares.abort",
            side_effect=CloseSpider(
                "OUT_DIR setting is required for ResponseSaverDLMW"
            ),
        )

        # Should abort when OUT_DIR is not set but SAVE_HTML is True
        with pytest.raises(CloseSpider):
            list(middleware.process_spider_output(response, [], spider))

        # Verify abort was called with the correct message
        mock_abort.assert_called_once()
        call_args = mock_abort.call_args
        assert "OUT_DIR setting is required" in str(call_args)

    def test_no_save_when_save_html_false(self, middleware, response, tmp_path):
        """Test middleware does nothing when SAVE_HTML is False."""
        spider = Spider(name="minimal_spider")
        spider.settings = Settings({"SAVE_HTML": False, "OUT_DIR": str(tmp_path)})
        spider.start_urls = []

        # Process the response with empty result
        result = list(middleware.process_spider_output(response, [], spider))

        # Verify no files were saved
        saved_files = list(tmp_path.glob("*.html"))
        assert len(saved_files) == 0
        assert result == []

    def test_saves_html_when_enabled(self, middleware, response, tmp_path):
        """Test middleware saves HTML when SAVE_HTML is True."""
        spider = Spider(name="test_spider")
        spider.settings = Settings({"SAVE_HTML": True, "OUT_DIR": str(tmp_path)})
        spider.start_urls = []

        # Process the response with empty result
        result = list(middleware.process_spider_output(response, [], spider))

        # Verify file was saved
        saved_files = list(tmp_path.glob("*.html"))
        assert len(saved_files) == 1
        assert saved_files[0].read_bytes() == response.body
        assert result == []

    def test_local_file_response(self, middleware, local_response, tmp_path):
        """Test middleware with local file:// URL response."""
        spider = Spider(name="local_spider")
        spider.settings = Settings({"SAVE_HTML": True, "OUT_DIR": str(tmp_path)})
        spider.start_urls = []

        # Process the response
        result = list(middleware.process_spider_output(local_response, [], spider))

        # Verify file was saved with original filename
        saved_files = list(tmp_path.glob("test.html"))
        assert len(saved_files) == 1
        assert saved_files[0].read_bytes() == local_response.body
        assert result == []

    def test_directory_creation(self, middleware, response, tmp_path):
        """Test that middleware creates output directory if it doesn't exist."""
        out_dir = tmp_path / "subdir" / "nested"
        spider = Spider(name="dir_creation_spider")
        spider.settings = Settings({"SAVE_HTML": True, "OUT_DIR": str(out_dir)})
        spider.start_urls = []

        # Verify directory doesn't exist yet
        assert not out_dir.exists()

        # Process the response
        list(middleware.process_spider_output(response, [], spider))

        # Verify directory was created
        assert out_dir.exists()
        assert out_dir.is_dir()

        # Verify file was saved
        saved_files = list(out_dir.glob("*.html"))
        assert len(saved_files) == 1

    def test_url_with_query_parameters(self, middleware, tmp_path):
        """Test middleware handles URLs with query parameters correctly."""
        response = HtmlResponse(
            url="http://example.com/page.html?id=123&sort=asc",
            body=b"<html><body>Query params</body></html>",
            request=Request(url="http://example.com/page.html?id=123&sort=asc"),
        )

        spider = Spider(name="query_spider")
        spider.settings = Settings({"SAVE_HTML": True, "OUT_DIR": str(tmp_path)})
        spider.start_urls = []

        # Process the response
        list(middleware.process_spider_output(response, [], spider))

        # Verify file was saved (query params removed from filename)
        saved_files = list(tmp_path.glob("page.html"))
        assert len(saved_files) == 1
        assert saved_files[0].read_bytes() == response.body

    def test_url_without_html_extension(self, middleware, tmp_path):
        """Test middleware adds .html extension to URLs without it."""
        response = HtmlResponse(
            url="http://example.com/api/data",
            body=b"<html><body>API response</body></html>",
            request=Request(url="http://example.com/api/data"),
        )

        spider = Spider(name="api_spider")
        spider.settings = Settings({"SAVE_HTML": True, "OUT_DIR": str(tmp_path)})
        spider.start_urls = []

        # Process the response
        list(middleware.process_spider_output(response, [], spider))

        # Verify file was saved with .html extension
        saved_files = list(tmp_path.glob("data.html"))
        assert len(saved_files) == 1
        assert saved_files[0].read_bytes() == response.body

    def test_local_mode_request_url_rewriting(self, middleware, response, tmp_path):
        """Test that Request URLs are rewritten to local files in local mode."""
        spider = Spider(name="local_mode_spider")
        spider.settings = Settings({"SAVE_HTML": True, "OUT_DIR": str(tmp_path)})
        spider.start_urls = ["file:///some/local/file.html"]  # Triggers local mode

        # Create a request that will be in the result
        next_request = Request(url="http://example.com/next.html")

        # Process the response with a request in the result
        result = list(
            middleware.process_spider_output(response, [next_request], spider)
        )

        # Verify the request URL was rewritten to point to the saved file
        assert len(result) == 1
        assert isinstance(result[0], Request)
        assert result[0].url.startswith("file://")
        assert str(tmp_path) in result[0].url

    def test_local_mode_does_not_rewrite_items(self, middleware, response, tmp_path):
        """Test that Items are not modified in local mode."""
        spider = Spider(name="local_mode_spider")
        spider.settings = Settings({"SAVE_HTML": True, "OUT_DIR": str(tmp_path)})
        spider.start_urls = ["file:///some/local/file.html"]  # Triggers local mode

        # Create an item that will be in the result
        item = {"title": "Test Item"}

        # Process the response with an item in the result
        result = list(middleware.process_spider_output(response, [item], spider))

        # Verify the item was not modified
        assert len(result) == 1
        assert result[0] == item

    def test_non_local_mode_does_not_rewrite_urls(self, middleware, response, tmp_path):
        """Test that Request URLs are not rewritten when not in local mode."""
        spider = Spider(name="non_local_spider")
        spider.settings = Settings({"SAVE_HTML": True, "OUT_DIR": str(tmp_path)})
        spider.start_urls = ["http://example.com"]  # Not local mode

        # Create a request that will be in the result
        next_request = Request(url="http://example.com/next.html")

        # Process the response with a request in the result
        result = list(
            middleware.process_spider_output(response, [next_request], spider)
        )

        # Verify the request URL was NOT rewritten
        assert len(result) == 1
        assert isinstance(result[0], Request)
        assert result[0].url == "http://example.com/next.html"

    def test_generate_filename_with_local_file(self, middleware):
        """Test _generate_filename with local file:// URL."""
        filename = middleware._generate_filename(
            "file:///home/user/documents/test.html"
        )
        assert filename == "test.html"

    def test_generate_filename_with_web_url(self, middleware):
        """Test _generate_filename with web URL."""
        filename = middleware._generate_filename("http://example.com/path/to/page.html")
        assert filename == "page.html"

    def test_generate_filename_with_query_params(self, middleware):
        """Test _generate_filename removes query parameters."""
        filename = middleware._generate_filename("http://example.com/page.html?id=123")
        assert filename == "page.html"

    def test_generate_filename_adds_html_extension(self, middleware):
        """Test _generate_filename adds .html extension when missing."""
        filename = middleware._generate_filename("http://example.com/api/data")
        assert filename == "data.html"

    def test_generate_filename_with_trailing_slash(self, middleware):
        """Test _generate_filename handles URLs with trailing slash."""
        filename = middleware._generate_filename("http://example.com/path/")
        assert filename == "path.html"

    def test_is_local_mode_with_local_url(self, middleware):
        """Test _is_local_mode returns True when start_urls contains file:// URL."""
        spider = Spider(name="test_spider")
        spider.start_urls = ["file:///path/to/file.html"]
        assert middleware._is_local_mode(spider) is True

    def test_is_local_mode_with_mixed_urls(self, middleware):
        """Test _is_local_mode returns True when at least one start_url is local."""
        spider = Spider(name="test_spider")
        spider.start_urls = ["http://example.com", "file:///path/to/file.html"]
        assert middleware._is_local_mode(spider) is True

    def test_is_local_mode_with_no_local_urls(self, middleware):
        """Test _is_local_mode returns False when no start_urls are local."""
        spider = Spider(name="test_spider")
        spider.start_urls = ["http://example.com", "https://example.org"]
        assert middleware._is_local_mode(spider) is False

    def test_is_local_mode_with_no_start_urls(self, middleware):
        """Test _is_local_mode returns False when spider has no start_urls."""
        spider = Spider(name="test_spider")
        assert middleware._is_local_mode(spider) is False


if __name__ == "__main__":
    pytest.main([__file__])
