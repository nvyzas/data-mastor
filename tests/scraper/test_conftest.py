from _pytest.fixtures import FixtureFunctionDefinition
from pytest import FixtureRequest

import data_mastor.scraper.testing as testing


def test_fixtures(request: FixtureRequest):
    """Make sure all fixtures from testing are exposed in conftest.py, and thus, are
    discoverable by pytest."""
    v = vars(testing)
    available = {k for k in v if isinstance(v[k], FixtureFunctionDefinition)}
    discovered = set(request._fixturemanager._arg2fixturedefs)
    for av in available:
        assert av in discovered
