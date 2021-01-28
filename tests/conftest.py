"""Global pytest fixtures."""
import os
from pathlib import Path

import pytest

TESTS_FOLDER = Path(__file__).parent
DOTENV_FILE = TESTS_FOLDER.joinpath(".env")
CONFIG_FILE = TESTS_FOLDER.joinpath("vig.config.json")
CSV_FOLDER = TESTS_FOLDER.joinpath("csv")
BACKUP_FOLDER = TESTS_FOLDER.joinpath("backup")
DB_FILE = TESTS_FOLDER.joinpath("vig_test.db")
SQLITE_URL = f"sqlite:///{DB_FILE}"


@pytest.fixture(scope="session", autouse=True)
def env_vars(request):
    """Sets environment variables to use .env and config.json files."""
    os.environ["ENV"] = "TEST"
    os.environ["DOTENV_FILE"] = str(DOTENV_FILE)
    os.environ["CONFIG_FILE"] = str(CONFIG_FILE)
    os.environ["DATABASE_URL"] = SQLITE_URL
    return True
