import os
from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture
from scrapy import Request

from data_mastor.scraper.middlewares import (
    ENVVAR_ALLOWED_INTERFACE,
    ENVVAR_NO_LEAK_TEST,
    ENVVAR_NO_UA_TEST,
    ENVVAR_PROXY_IP,
    PrivacyCheckerDLMW,
)


@pytest.fixture
def mock_spider():
    """Create a mock spider for unit testing."""
    spider = Mock()
    spider.name = "test_spider"
    spider.logger = Mock()
    return spider


@pytest.fixture(params=["uatestY", "uatestN"])
def env_no_ua_test(request):
    os.environ[ENVVAR_NO_UA_TEST] = "" if request.param == "uatestY" else "true"


@pytest.fixture(params=["proxyY", "proxyN"])
def env_proxy_ip(request):
    os.environ[ENVVAR_PROXY_IP] = "1.2.3.4" if request.param == "proxyY" else ""


@pytest.fixture(params=["leaktestY", "leaktestN"])
def env_no_leak_test(request):
    os.environ[ENVVAR_NO_LEAK_TEST] = "" if request.param == "leaktestY" else "true"


@pytest.fixture(params=["allowedinterfaceY", "allowedinterfaceN"])
def env_allowed_interface(request):
    os.environ[ENVVAR_ALLOWED_INTERFACE] = (
        "allowedinterface" if request.param == "allowedinterfaceY" else ""
    )


@pytest.mark.usefixtures(
    env_proxy_ip.__name__,
    env_allowed_interface.__name__,
    env_no_ua_test.__name__,
    env_no_leak_test.__name__,
)
def test_spider_opened_no_proxy_no_interface(
    mock_spider, mocker: MockerFixture
):
    """Test spider_opened when no proxy or interface is configured."""
    mock_is_leaking = mocker.patch("data_mastor.scraper.middlewares._is_leaking")
    mock_is_leaking.return_value = False  # No leaks detected
    mock_interface_is_up = mocker.patch("data_mastor.scraper.middlewares._interface_is_up")
    mock_interface_ip = mocker.patch("data_mastor.scraper.middlewares._interface_ip")
    mock_abort = mocker.patch("data_mastor.scraper.middlewares.abort")
    
    middleware = PrivacyCheckerDLMW()
    middleware.spider_opened(mock_spider)
    
    # When no proxy or interface is configured (empty env vars), checks should not be called
    if not os.environ.get(ENVVAR_PROXY_IP) and not os.environ.get(ENVVAR_ALLOWED_INTERFACE):
        assert not mock_interface_is_up.called
        assert not mock_interface_ip.called
        # When leak test is enabled and no proxy/interface, leak test should run
        if not os.environ.get(ENVVAR_NO_LEAK_TEST):
            assert mock_is_leaking.called
        assert not mock_abort.called


@pytest.mark.usefixtures(env_no_leak_test.__name__)
def test_spider_opened_with_leaktest(
    mock_spider, mocker: MockerFixture
):
    """Test spider_opened with leak test configuration."""
    mock_is_leaking = mocker.patch("data_mastor.scraper.middlewares._is_leaking")
    mock_abort = mocker.patch("data_mastor.scraper.middlewares.abort")
    mock_abort.side_effect = RuntimeError("Test abort called")
    
    # Set proxy for testing
    os.environ[ENVVAR_PROXY_IP] = "1.2.3.4"
    
    middleware = PrivacyCheckerDLMW()
    
    # Determine expected behavior
    should_run_leaktest = not os.environ.get(ENVVAR_NO_LEAK_TEST, "")
    should_abort = should_run_leaktest and mock_is_leaking.return_value
    
    # Call spider_opened, catching abort exception if expected
    if should_abort:
        with pytest.raises(RuntimeError, match="Test abort called"):
            middleware.spider_opened(mock_spider)
    else:
        middleware.spider_opened(mock_spider)
    
    # Check behavior based on environment variable
    if not should_run_leaktest:
        assert not mock_is_leaking.called
        assert not mock_abort.called
    else:
        # Leak test should have been called for proxy
        assert mock_is_leaking.called
        if should_abort:
            assert mock_abort.called


def test_process_request_with_proxy(mock_spider, mocker: MockerFixture):
    """Test process_request sets proxy correctly."""
    middleware = PrivacyCheckerDLMW()
    middleware._check_ua = False
    middleware.proxy_ip = "http://proxy:8080"
    middleware.interface_ip = ""
    
    request = Request("http://example.com")
    result = middleware.process_request(request, mock_spider)
    
    assert result is None
    assert request.meta["proxy"] == "http://proxy:8080"


def test_process_request_with_interface(mock_spider, mocker: MockerFixture):
    """Test process_request sets bindaddress correctly."""
    middleware = PrivacyCheckerDLMW()
    middleware._check_ua = False
    middleware.proxy_ip = ""
    middleware.interface_ip = "192.168.1.100"
    
    request = Request("http://example.com")
    result = middleware.process_request(request, mock_spider)
    
    assert result is None
    assert request.meta["bindaddress"] == "192.168.1.100"


def test_process_request_user_agent_check(mock_spider, mocker: MockerFixture):
    """Test process_request checks User-Agent header."""
    mock_abort = mocker.patch("data_mastor.scraper.middlewares.abort")
    mock_abort.side_effect = RuntimeError("Test abort called")
    
    middleware = PrivacyCheckerDLMW()
    middleware._check_ua = True
    middleware.proxy_ip = ""
    middleware.interface_ip = ""
    
    # Test with missing User-Agent
    request = Request("http://example.com")
    with pytest.raises(RuntimeError, match="Test abort called"):
        middleware.process_request(request, mock_spider)
    assert mock_abort.called
    
    # Test with bad User-Agent (contains "bot")
    mock_abort.reset_mock()
    request = Request("http://example.com", headers={"User-Agent": "mybot"})
    with pytest.raises(RuntimeError, match="Test abort called"):
        middleware.process_request(request, mock_spider)
    assert mock_abort.called
    
    # Test with good User-Agent
    mock_abort.reset_mock()
    request = Request("http://example.com", headers={"User-Agent": "Mozilla/5.0"})
    result = middleware.process_request(request, mock_spider)
    assert result is None
    assert not mock_abort.called


if __name__ == "__main__":
    pytest.main([__file__])
