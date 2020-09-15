"""Global pytest fixtures."""
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from vigorish.config.config_file import ConfigFile
from vigorish.config.database import initialize_database
from vigorish.config.dotenv_file import DotEnvFile, create_default_dotenv_file
from vigorish.data.scraped_data import ScrapedData

from tests.util import seed_database_with_test_data

TESTS_FOLDER = Path(__file__).parent
DOTENV_FILE = TESTS_FOLDER.joinpath(".env")
CONFIG_FILE = TESTS_FOLDER.joinpath("vig.config.json")
SQLITE_URL = "sqlite://"


@pytest.fixture(scope="session")
def dotenv(request):
    if DOTENV_FILE.exists():
        return DotEnvFile(dotenv_filepath=DOTENV_FILE)
    create_default_dotenv_file(dotenv_file=DOTENV_FILE, config_file=CONFIG_FILE, db_url=SQLITE_URL)
    return DotEnvFile(dotenv_filepath=DOTENV_FILE)


@pytest.fixture(scope="session")
def config(dotenv, request):
    return ConfigFile(config_file_path=CONFIG_FILE)


@pytest.fixture(scope="session")
def db_engine(request):
    return create_engine(SQLITE_URL)


@pytest.fixture(scope="session")
def db_session(request, db_engine):
    session_maker = sessionmaker(bind=db_engine)
    db_session = session_maker()

    def fin():
        db_session.close()

    request.addfinalizer(fin)
    return db_session


@pytest.fixture(scope="session")
def scraped_data(config, db_engine, db_session):
    return ScrapedData(db_engine=db_engine, db_session=db_session, config=config)


@pytest.fixture(scope="session", autouse=True)
def seed_db(request, dotenv, config, db_engine, db_session, scraped_data):
    app = {
        "dotenv": dotenv,
        "config": config,
        "db_engine": db_engine,
        "db_session": db_session,
        "scraped_data": scraped_data,
    }
    initialize_database(app, is_test=True)
    seed_database_with_test_data(db_session, scraped_data)
