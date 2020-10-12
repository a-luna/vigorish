"""Global pytest fixtures."""
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from vigorish.config.config_file import ConfigFile
from vigorish.config.database import initialize_database
from vigorish.config.dotenv_file import create_default_dotenv_file, DotEnvFile
from vigorish.data.scraped_data import ScrapedData

TESTS_FOLDER = Path(__file__).parent
DOTENV_FILE = TESTS_FOLDER.joinpath(".env")
CONFIG_FILE = TESTS_FOLDER.joinpath("vig.config.json")
CSV_FOLDER = TESTS_FOLDER.joinpath("csv")
DB_FILE = TESTS_FOLDER.joinpath("vig_test.db")
SQLITE_URL = f"sqlite:///{DB_FILE}"


@pytest.fixture(scope="module")
def dotenv(request):
    """Returns a DotEnvFile instance using the .env file in the tests folder."""
    if DOTENV_FILE.exists():
        return DotEnvFile(dotenv_filepath=DOTENV_FILE)
    create_default_dotenv_file(dotenv_file=DOTENV_FILE, config_file=CONFIG_FILE, db_url=SQLITE_URL)
    return DotEnvFile(dotenv_filepath=DOTENV_FILE)


@pytest.fixture(scope="module")
def config(dotenv, request):
    """Returns a ConfigFile instance using the vig.config.json file in the tests folder."""
    return ConfigFile(config_file_path=CONFIG_FILE)


@pytest.fixture(scope="module")
def db_engine(request):
    """Returns a new SQLAlchemy Engine instance configured with a in-memory SQLite database."""
    return create_engine(SQLITE_URL)


@pytest.fixture(scope="module")
def db_session(request, db_engine):
    """Returns a SQLAlchemy session object that allows interaction with the in-memory SQLite DB."""
    session_maker = sessionmaker(bind=db_engine)
    db_session = session_maker()

    def fin():
        db_session.close()

    request.addfinalizer(fin)
    return db_session


@pytest.fixture(scope="module")
def scraped_data(config, db_engine, db_session):
    """Returns a ScrapedData instance using the test HTML/JSON folders and in-memory SQLite DB."""
    return ScrapedData(db_engine=db_engine, db_session=db_session, config=config)


@pytest.fixture(scope="module")
def vig_app(dotenv, config, db_engine, db_session, scraped_data):
    """Returns a dict containing the same attributes/objects as the app object used in the CLI."""
    return {
        "dotenv": dotenv,
        "config": config,
        "db_engine": db_engine,
        "db_session": db_session,
        "scraped_data": scraped_data,
    }


@pytest.fixture(scope="module", autouse=True)
def setup_db(vig_app):
    """Creates all schema-defined tables and views in a SQLite database."""
    initialize_database(vig_app, csv_folder=CSV_FOLDER)
    return True
