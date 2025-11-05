import os

import pytest
from pytest_mock import MockerFixture
from scrapy import Request

from data_mastor.scraper.middlewares import (
    ENVVAR_ALLOWED_INTERFACE,
    ENVVAR_NO_LEAK_TEST,
    ENVVAR_NO_UA_TEST,
    ENVVAR_PROXY_IP,
    PrivacyCheckerDLMW,
    _interface_ip,
    _interface_is_up,
    _is_leaking,
)
from data_mastor.scraper.utils import abort


@pytest.fixture
def mock_spider(mocker: MockerFixture):
    """Create a mock spider for unit testing using pytest_mock."""
    spider = mocker.Mock()
    spider.name = "test_spider"
    spider.logger = mocker.Mock()
    return spider


@pytest.fixture
def middleware():
    """Create a middleware instance for testing."""
    return PrivacyCheckerDLMW()


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


@pytest.fixture
def mock_interface_ip(mocker: MockerFixture):
    """Mock the _interface_ip function."""
    return mocker.patch(
        "data_mastor.scraper.middlewares._interface_ip", autospec=_interface_ip
    )


@pytest.fixture
def mock_interface_is_up(mocker: MockerFixture):
    """Mock the _interface_is_up function."""
    return mocker.patch(
        "data_mastor.scraper.middlewares._interface_is_up", autospec=_interface_is_up
    )


@pytest.fixture(params=["leaksY", "leaksN"])
def mock_is_leaking(mocker: MockerFixture, request):
    """Mock the _is_leaking function."""
    mock = mocker.patch(
        "data_mastor.scraper.middlewares._is_leaking", autospec=_is_leaking
    )
    mock.return_value = True if request.param == "leaksY" else False
    return mock


@pytest.fixture
def mock_abort(mocker: MockerFixture):
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
    proxy_passed_leak_test = proxy_ip and not (leak_test_enabled and mock_is_leaking.return_value)
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


def test_process_request(mock_spider, middleware, mocker: MockerFixture):
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
    mock_abort_local = mocker.patch("data_mastor.scraper.middlewares.abort", autospec=abort)
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


if __name__ == "__main__":
    pytest.main([__file__])
