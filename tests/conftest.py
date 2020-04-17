"""Global pytest fixtures."""
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from vigorish.config.config_file import ConfigFile
from vigorish.data.scraped_data import ScrapedData

TESTS_FOLDER = Path(__file__).parent
CONFIG = TESTS_FOLDER / "vig.config.json"
SQLITE_URL = f"sqlite:///{TESTS_FOLDER / 'vig_test.db'}"


@pytest.fixture(scope="session")
def config(request):
    return ConfigFile(config_file_path=CONFIG)


@pytest.fixture(scope="session")
def db_session(request):
    db_engine = create_engine(SQLITE_URL)
    session_maker = sessionmaker(bind=db_engine)
    db_session = session_maker()

    def fin():
        db_session.close()

    request.addfinalizer(fin)
    return db_session


@pytest.fixture(scope="session")
def scraped_data(config, db_session):
    return ScrapedData(db=db_session, config=config)
