import os
import tempfile
from unittest.mock import MagicMock, PropertyMock

import pytest
from pytest_mock import MockerFixture

from data_mastor.scraper.middlewares import (
    ENVVAR_ALLOWED_INTERFACE,
    ENVVAR_NO_LEAK_TEST,
    ENVVAR_NO_UA_TEST,
    ENVVAR_PROXY_IP,
    PrivacyCheckerDLMW,
)
from data_mastor.scraper.spiders import Baze
from data_mastor.scraper.utils import abort


@pytest.fixture
def spider_instance():
    """Create a spider instance directly for unit testing."""
    fp = tempfile.NamedTemporaryFile("r", delete=False)
    spider = Baze(url=f"file://{fp.name}")
    yield spider
    fp.close()
    os.unlink(fp.name)


@pytest.fixture
def middleware():
    """Create a middleware instance for unit testing."""
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
def mock_nonlocal_mode(spider_instance, mocker: MockerFixture):
    """Mock the spider's local_mode property to return False."""
    mock = mocker.patch.object(
        type(spider_instance), "local_mode", new_callable=PropertyMock
    )
    mock.return_value = False
    return mock


@pytest.fixture
def mock_interface_ip(mocker: MockerFixture) -> MagicMock:
    return mocker.patch("data_mastor.scraper.middlewares._interface_ip")


@pytest.fixture
def mock_interface_is_up(mocker: MockerFixture) -> MagicMock:
    return mocker.patch("data_mastor.scraper.middlewares._interface_is_up")


@pytest.fixture(params=["leaksY", "leaksN"])
def mock_is_leaking(mocker: MockerFixture, request) -> MagicMock:
    mock = mocker.patch("data_mastor.scraper.middlewares._is_leaking")
    mock.return_value = True if request.param == "leaksY" else False
    return mock


@pytest.fixture
def mock_abort(mocker: MockerFixture) -> MagicMock:
    """Mock the abort function to raise an exception for testing."""
    mock = mocker.patch("data_mastor.scraper.middlewares.abort")
    # Don't set side_effect to the real abort - let it be a simple mock
    # This allows tests to verify abort was called without actually aborting
    mock.side_effect = RuntimeError("Test abort called")
    return mock


@pytest.mark.usefixtures(
    env_proxy_ip.__name__,
    env_allowed_interface.__name__,
    env_no_ua_test.__name__,
    env_no_leak_test.__name__,
)
def test_local(spider_instance, middleware, mock_is_leaking, mock_interface_is_up, mock_interface_ip):
    """Test that privacy checks are skipped in local mode."""
    # Spider is in local mode by default (file:// URL)
    assert spider_instance.local_mode is True
    
    # Call spider_opened directly without starting the reactor
    middleware.spider_opened(spider_instance)
    
    # Assert that privacy checks were not called in local mode
    assert not mock_is_leaking.called
    assert not mock_interface_is_up.called
    assert not mock_interface_ip.called


@pytest.mark.usefixtures(env_no_leak_test.__name__)
def test_leaktest(spider_instance, middleware, mock_nonlocal_mode, mock_is_leaking, mock_abort):
    """Test that leak tests run correctly in non-local mode."""
    # Mock the spider to be in non-local mode
    assert mock_nonlocal_mode.return_value is False
    
    # Determine expected behavior before calling spider_opened
    should_run_leaktest = not os.environ[ENVVAR_NO_LEAK_TEST]
    should_abort = should_run_leaktest and mock_is_leaking.return_value
    
    # Call spider_opened, catching abort exception if expected
    if should_abort:
        with pytest.raises(RuntimeError, match="Test abort called"):
            middleware.spider_opened(spider_instance)
    else:
        middleware.spider_opened(spider_instance)
    
    # The property should have been accessed
    assert mock_nonlocal_mode.called
    
    # Check behavior based on environment variable
    if not should_run_leaktest:
        assert not mock_is_leaking.called
        assert not mock_abort.called
        return
    
    # Leak test should have been called
    assert mock_is_leaking.called
    
    # Check if abort was called based on leak detection
    if should_abort:
        assert mock_abort.called
    else:
        assert not mock_abort.called


if __name__ == "__main__":
    pytest.main([__file__])
