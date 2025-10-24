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
)
from data_mastor.scraper.spiders import Baze
from data_mastor.scraper.utils import abort


@pytest.fixture(scope="module")  # module scope to prevent creation of many temp files
def spidercls():
    fp = tempfile.NamedTemporaryFile("r")
    Baze._spiderargs["url"] = f"file://{fp.name}"
    yield Baze
    fp.close()


@pytest.fixture
def yamlargs():
    return {}


@pytest.fixture(autouse=True)
def configure_spidercls(configure_spidercls):
    pass


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
def mock_nonlocal_mode(spidercls, mocker: MockerFixture):
    mock = mocker.patch.object(
        spidercls, spidercls.local_mode.__name__, new_callable=PropertyMock
    )
    mock.return_value = False
    return mock


@pytest.fixture(autouse=True)
def mock_parse(spidercls, mocker: MockerFixture):
    def parse(self, response):
        yield self, response

    mock = mocker.patch.object(spidercls, spidercls.parse.__name__, parse)
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
def mock_abort(mocker: MockerFixture, request) -> MagicMock:
    mock = mocker.patch("data_mastor.scraper.middlewares.abort")
    mock.side_effect = abort
    return mock


@pytest.mark.forked
@pytest.mark.usefixtures(
    env_proxy_ip.__name__,
    env_allowed_interface.__name__,
    env_no_ua_test.__name__,
    env_no_leak_test.__name__,
)
def test_local(spidercls, mock_is_leaking, mock_interface_is_up, mock_interface_ip):
    spidercls.main()
    assert not mock_is_leaking.called
    assert not mock_interface_is_up.called
    assert not mock_interface_ip.called


@pytest.mark.forked
@pytest.mark.usefixtures(env_no_leak_test.__name__)
def test_leaktest(spidercls, mock_nonlocal_mode, mock_is_leaking, mock_abort):
    spidercls.main()
    assert mock_nonlocal_mode.called
    if os.environ[ENVVAR_NO_LEAK_TEST]:
        assert not mock_is_leaking.called
        assert not mock_abort.called
        return
    assert mock_is_leaking.called
    if mock_is_leaking.return_value:
        assert mock_abort.called
    else:
        assert not mock_abort.called


if __name__ == "__main__":
    pytest.main([__file__])
