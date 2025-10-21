import os
from collections.abc import Iterable

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session

from data_mastor.dbman import get_engine
from data_mastor.scraper.models import Base


@pytest.fixture(autouse=True, scope="session")
def setup_db_url():
    os.environ["DB_URL"] = "sqlite:///:memory:"


@pytest.fixture(scope="session")
def engine():
    """Create a new database engine for the test session."""
    engine = get_engine()
    Base.metadata.create_all(engine)
    yield engine


@pytest.fixture(scope="session")
def Sessionmaker(engine: Engine):
    """Create a new sessionmaker for the test session (scope same as engine)."""
    Sessionmaker = sessionmaker(bind=engine)
    yield Sessionmaker


@pytest.fixture(scope="function")
def session(Sessionmaker: sessionmaker[Session]):
    session: Session = Sessionmaker()

    yield session

    session.close()


@pytest.fixture()
def session_add(session: Session, items: Iterable[Base]):
    session.add_all(items)
