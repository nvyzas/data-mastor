"""Unit tests for ResponseSaverDLMW middleware."""

import tempfile
from pathlib import Path

import pytest
from scrapy import Spider
from scrapy.exceptions import CloseSpider
from scrapy.http import HtmlResponse, Request
from scrapy.settings import Settings

from data_mastor.scraper.middlewares import ResponseSaverDLMW


class TestResponseSaverDLMW:
    """Test the ResponseSaverDLMW middleware."""

    @pytest.fixture
    def middleware(self):
        """Create a ResponseSaverDLMW instance."""
        return ResponseSaverDLMW()

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

    def test_requires_out_dir_setting(
        self, middleware, scrapy_request, response, mocker
    ):
        """Test that middleware aborts if OUT_DIR setting is not set."""
        spider = mocker.Mock(spec=Spider)
        spider.name = "test_spider"
        spider.settings = Settings()
        spider.logger = mocker.Mock()
        spider.crawler = mocker.Mock()

        # Mock abort to raise CloseSpider
        mock_abort = mocker.patch(
            "data_mastor.scraper.middlewares.abort",
            side_effect=CloseSpider(
                "OUT_DIR setting is required for ResponseSaverDLMW"
            ),
        )

        # Should abort when OUT_DIR is not set
        with pytest.raises(CloseSpider):
            middleware.process_response(scrapy_request, response, spider)

        # Verify abort was called with the correct message
        mock_abort.assert_called_once()
        call_args = mock_abort.call_args
        assert "OUT_DIR setting is required" in str(call_args)

    def test_minimal_spider_with_out_dir(
        self, middleware, scrapy_request, response, tmp_path
    ):
        """Test middleware with a minimal spider that has OUT_DIR setting."""
        spider = Spider(name="minimal_spider")
        spider.settings = Settings({"OUT_DIR": str(tmp_path)})

        # Process the response
        result = middleware.process_response(scrapy_request, response, spider)

        # Verify response is returned unchanged
        assert result is response

        # Verify file was saved
        saved_files = list(tmp_path.glob("*.html"))
        assert len(saved_files) == 1
        assert saved_files[0].read_bytes() == response.body

    def test_local_file_response(self, middleware, local_response, tmp_path):
        """Test middleware with local file:// URL response."""
        spider = Spider(name="local_spider")
        spider.settings = Settings({"OUT_DIR": str(tmp_path)})

        scrapy_request = Request(url="file:///tmp/test.html")

        # Process the response
        result = middleware.process_response(scrapy_request, local_response, spider)

        # Verify response is returned unchanged
        assert result is local_response

        # Verify file was saved with original filename
        saved_files = list(tmp_path.glob("test.html"))
        assert len(saved_files) == 1
        assert saved_files[0].read_bytes() == local_response.body

    def test_directory_creation(self, middleware, scrapy_request, response, tmp_path):
        """Test that middleware creates output directory if it doesn't exist."""
        out_dir = tmp_path / "subdir" / "nested"
        spider = Spider(name="dir_creation_spider")
        spider.settings = Settings({"OUT_DIR": str(out_dir)})

        # Verify directory doesn't exist yet
        assert not out_dir.exists()

        # Process the response
        middleware.process_response(scrapy_request, response, spider)

        # Verify directory was created
        assert out_dir.exists()
        assert out_dir.is_dir()

        # Verify file was saved
        saved_files = list(out_dir.glob("*.html"))
        assert len(saved_files) == 1

    def test_url_with_query_parameters(self, middleware, scrapy_request, tmp_path):
        """Test middleware handles URLs with query parameters correctly."""
        response = HtmlResponse(
            url="http://example.com/page.html?id=123&sort=asc",
            body=b"<html><body>Query params</body></html>",
            request=scrapy_request,
        )

        spider = Spider(name="query_spider")
        spider.settings = Settings({"OUT_DIR": str(tmp_path)})

        # Process the response
        middleware.process_response(scrapy_request, response, spider)

        # Verify file was saved (query params removed from filename)
        saved_files = list(tmp_path.glob("page.html"))
        assert len(saved_files) == 1
        assert saved_files[0].read_bytes() == response.body

    def test_url_without_html_extension(self, middleware, scrapy_request, tmp_path):
        """Test middleware adds .html extension to URLs without it."""
        response = HtmlResponse(
            url="http://example.com/api/data",
            body=b"<html><body>API response</body></html>",
            request=scrapy_request,
        )

        spider = Spider(name="api_spider")
        spider.settings = Settings({"OUT_DIR": str(tmp_path)})

        # Process the response
        middleware.process_response(scrapy_request, response, spider)

        # Verify file was saved with .html extension
        saved_files = list(tmp_path.glob("data.html"))
        assert len(saved_files) == 1
        assert saved_files[0].read_bytes() == response.body

    def test_backward_compatibility_with_baze_spider(
        self, middleware, scrapy_request, response, tmp_path
    ):
        """Test that middleware works with Baze spiders (backward compatibility)."""
        from data_mastor.scraper.spiders import Baze

        # Store original values to restore later
        original_spiderargs = Baze._spiderargs.copy()
        original_settings = Baze._settings.copy()

        # Create a temporary file for the spider
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".html") as f:
            f.write("<html><body>Test</body></html>")
            temp_file = f.name

        try:
            # Configure Baze spider
            Baze._spiderargs = {"url": f"file://{temp_file}", "save_html": False}
            Baze._settings = {"OUT_DIR": str(tmp_path)}

            # Create spider instance
            spider = Baze()
            spider.settings = Settings({"OUT_DIR": str(tmp_path)})

            # Process the response
            result = middleware.process_response(scrapy_request, response, spider)

            # Verify response is returned unchanged
            assert result is response

            # Verify file was saved
            saved_files = list(tmp_path.glob("*.html"))
            assert len(saved_files) == 1
            assert saved_files[0].read_bytes() == response.body

        finally:
            # Cleanup: restore original values and remove temp file
            Path(temp_file).unlink(missing_ok=True)
            Baze._spiderargs = original_spiderargs
            Baze._settings = original_settings

    def test_generate_filename_with_local_file(self, middleware):
        """Test _generate_filename with local file:// URL."""
        response = HtmlResponse(
            url="file:///home/user/documents/test.html",
            body=b"<html></html>",
            request=Request(url="file:///home/user/documents/test.html"),
        )

        filename = middleware._generate_filename(response)
        assert filename == "test.html"

    def test_generate_filename_with_web_url(self, middleware):
        """Test _generate_filename with web URL."""
        response = HtmlResponse(
            url="http://example.com/path/to/page.html",
            body=b"<html></html>",
            request=Request(url="http://example.com/path/to/page.html"),
        )

        filename = middleware._generate_filename(response)
        assert filename == "page.html"

    def test_generate_filename_with_query_params(self, middleware):
        """Test _generate_filename removes query parameters."""
        response = HtmlResponse(
            url="http://example.com/page.html?id=123",
            body=b"<html></html>",
            request=Request(url="http://example.com/page.html?id=123"),
        )

        filename = middleware._generate_filename(response)
        assert filename == "page.html"

    def test_generate_filename_adds_html_extension(self, middleware):
        """Test _generate_filename adds .html extension when missing."""
        response = HtmlResponse(
            url="http://example.com/api/data",
            body=b"<html></html>",
            request=Request(url="http://example.com/api/data"),
        )

        filename = middleware._generate_filename(response)
        assert filename == "data.html"

    def test_generate_filename_with_trailing_slash(self, middleware):
        """Test _generate_filename handles URLs with trailing slash."""
        response = HtmlResponse(
            url="http://example.com/path/",
            body=b"<html></html>",
            request=Request(url="http://example.com/path/"),
        )

        filename = middleware._generate_filename(response)
        assert filename == "path.html"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
