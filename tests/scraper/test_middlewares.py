import os
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from scrapy import Request, Spider
from scrapy.exceptions import CloseSpider
from scrapy.http import HtmlResponse
from scrapy.settings import Settings

from data_mastor.scraper.middlewares import (
    ENVVAR_ALLOWED_INTERFACE,
    ENVVAR_NO_LEAK_TEST,
    ENVVAR_NO_UA_TEST,
    ENVVAR_PROXY_IP,
    PrivacyCheckerDlMw,
    ResponseSaverSpMw,
    _interface_ip,
    _interface_is_up,
    _is_leaking,
)
from data_mastor.scraper.utils import abort


class TestPrivacyCheckerDlMw:
    @pytest.fixture
    def mock_spider(self, mocker: MockerFixture) -> MagicMock:
        """Create a mock spider for unit testing using pytest_mock."""
        spider = mocker.Mock()
        spider.name = "test_spider"
        spider.logger = mocker.Mock()
        return spider

    @pytest.fixture
    def middleware(self):
        """Create a middleware instance for testing."""
        return PrivacyCheckerDlMw()

    @pytest.fixture(params=["uatestY", "uatestN"])
    def env_no_ua_test(self, request):
        os.environ[ENVVAR_NO_UA_TEST] = "" if request.param == "uatestY" else "true"

    @pytest.fixture(params=["proxyY", "proxyN"])
    def env_proxy_ip(self, request):
        os.environ[ENVVAR_PROXY_IP] = "1.2.3.4" if request.param == "proxyY" else ""

    @pytest.fixture(params=["leaktestY", "leaktestN"])
    def env_no_leak_test(self, request):
        os.environ[ENVVAR_NO_LEAK_TEST] = "" if request.param == "leaktestY" else "true"

    @pytest.fixture(params=["allowedinterfaceY", "allowedinterfaceN"])
    def env_allowed_interface(self, request):
        os.environ[ENVVAR_ALLOWED_INTERFACE] = (
            "allowedinterface" if request.param == "allowedinterfaceY" else ""
        )

    @pytest.fixture
    def mock_interface_ip(self, mocker: MockerFixture):
        """Mock the _interface_ip function."""
        return mocker.patch(
            "data_mastor.scraper.middlewares._interface_ip", autospec=_interface_ip
        )

    @pytest.fixture
    def mock_interface_is_up(self, mocker: MockerFixture):
        """Mock the _interface_is_up function."""
        return mocker.patch(
            "data_mastor.scraper.middlewares._interface_is_up",
            autospec=_interface_is_up,
        )

    @pytest.fixture(params=["leaksY", "leaksN"])
    def mock_is_leaking(self, mocker: MockerFixture, request):
        """Mock the _is_leaking function."""
        mock = mocker.patch(
            "data_mastor.scraper.middlewares._is_leaking", autospec=_is_leaking
        )
        mock.return_value = True if request.param == "leaksY" else False
        return mock

    @pytest.fixture
    def mock_abort(self, mocker: MockerFixture):
        """Mock the abort function to raise an exception for testing."""
        mock = mocker.patch("data_mastor.scraper.middlewares.abort", autospec=abort)
        mock.side_effect = RuntimeError("Test abort called")
        return mock

    @pytest.mark.usefixtures(
        env_proxy_ip.__name__,
        env_allowed_interface.__name__,
        env_no_ua_test.__name__,
        env_no_leak_test.__name__,
    )
    def test_spider_opened(
        self,
        mock_spider,
        middleware,
        mock_is_leaking,
        mock_interface_is_up,
        mock_interface_ip,
        mock_abort,
    ):
        """Test spider_opened method with various environment configurations.

        Tests the middleware initialization logic including:
        - User-agent check configuration
        - Proxy configuration and leak testing
        - Network interface configuration and leak testing
        - Abort behavior on leak detection

        The middleware follows this flow:
        1. Check UA configuration
        2. If proxy configured: run proxy leak test (if enabled)
        3. If no proxy: check interface (if configured), then run regular leak test (if enabled)
        4. Abort if leak detected
        """
        # Get current environment configuration
        proxy_ip = os.environ.get(ENVVAR_PROXY_IP)
        interface = os.environ.get(ENVVAR_ALLOWED_INTERFACE)
        leak_test_enabled = not os.environ.get(ENVVAR_NO_LEAK_TEST)

        # Determine expected behavior
        should_abort = leak_test_enabled and mock_is_leaking.return_value

        # Call spider_opened, catching abort exception if expected
        if should_abort:
            with pytest.raises(RuntimeError, match="Test abort called"):
                middleware.spider_opened(mock_spider)
        else:
            middleware.spider_opened(mock_spider)

        # Verify user-agent check was configured
        ua_check_enabled = not os.environ.get(ENVVAR_NO_UA_TEST)
        assert middleware._check_ua == ua_check_enabled

        # Verify leak test behavior
        # Leak test always runs when enabled (either for proxy or for regular network)
        if leak_test_enabled:
            assert mock_is_leaking.called
        else:
            assert not mock_is_leaking.called

        # Verify interface checks behavior
        # Interface checks run when interface is configured AND (no proxy OR proxy leak test failed)
        # When proxy leak test fails, proxy_ip is not set on middleware, so interface checks run
        proxy_passed_leak_test = proxy_ip and not (
            leak_test_enabled and mock_is_leaking.return_value
        )
        if interface and not proxy_passed_leak_test:
            assert mock_interface_is_up.called
            assert mock_interface_ip.called
        else:
            assert not mock_interface_is_up.called
            assert not mock_interface_ip.called

        # Verify abort behavior
        if should_abort:
            assert mock_abort.called
        else:
            assert not mock_abort.called

    def test_process_request(self, mock_spider, middleware, mocker: MockerFixture):
        """Test process_request method with various configurations.

        Tests the request processing logic including:
        - Proxy configuration (sets request.meta["proxy"])
        - Interface/bindaddress configuration (sets request.meta["bindaddress"])
        - User-Agent header validation (aborts on missing or bad UA)
        - Proper precedence (proxy takes priority over interface)
        """
        # Test 1: Proxy configuration
        middleware._check_ua = False
        middleware.proxy_ip = "http://proxy:8080"
        middleware.interface_ip = ""

        request = Request("http://example.com")
        result = middleware.process_request(request, mock_spider)

        assert result is None
        assert request.meta["proxy"] == "http://proxy:8080"
        assert "bindaddress" not in request.meta

        # Test 2: Interface/bindaddress configuration
        middleware.proxy_ip = ""
        middleware.interface_ip = "192.168.1.100"

        request = Request("http://example.com")
        result = middleware.process_request(request, mock_spider)

        assert result is None
        assert request.meta["bindaddress"] == "192.168.1.100"
        assert "proxy" not in request.meta

        # Test 3: User-Agent validation - missing User-Agent
        mock_abort_local = mocker.patch(
            "data_mastor.scraper.middlewares.abort", autospec=abort
        )
        mock_abort_local.side_effect = RuntimeError("Test abort called")

        middleware._check_ua = True
        middleware.proxy_ip = ""
        middleware.interface_ip = ""

        request = Request("http://example.com")
        with pytest.raises(RuntimeError, match="Test abort called"):
            middleware.process_request(request, mock_spider)
        assert mock_abort_local.called

        # Test 4: User-Agent validation - bad User-Agent (contains "bot")
        mock_abort_local.reset_mock()
        request = Request("http://example.com", headers={"User-Agent": "mybot"})
        with pytest.raises(RuntimeError, match="Test abort called"):
            middleware.process_request(request, mock_spider)
        assert mock_abort_local.called

        # Test 5: User-Agent validation - good User-Agent
        mock_abort_local.reset_mock()
        request = Request("http://example.com", headers={"User-Agent": "Mozilla/5.0"})
        result = middleware.process_request(request, mock_spider)
        assert result is None
        assert not mock_abort_local.called


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
