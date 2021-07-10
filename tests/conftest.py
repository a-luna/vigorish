"""Global pytest fixtures."""
import os
from pathlib import Path
from uuid import uuid4

import pytest

TESTS_FOLDER = Path(__file__).parent
ROOT_FOLDER = TESTS_FOLDER.parent
DOTENV_FILE = TESTS_FOLDER.joinpath(".env")
CONFIG_FILE = TESTS_FOLDER.joinpath("vig.config.json")
JSON_FOLDER = TESTS_FOLDER.joinpath("json")
CSV_FOLDER = TESTS_FOLDER.joinpath("csv")
BACKUP_FOLDER = TESTS_FOLDER.joinpath("backup")


@pytest.fixture(scope="package", autouse=True)
def env_vars(request):
    """Sets environment variables to use .env and config.json files."""
    os.environ["ENV"] = "TEST"
    os.environ["DOTENV_FILE"] = str(DOTENV_FILE)
    os.environ["CONFIG_FILE"] = str(CONFIG_FILE)
    os.environ["DATABASE_URL"] = get_db_url()
    return True


def get_db_filename():
    return f"vig_{str(uuid4())[-4:]}.db"


def get_db_url():
    return f"sqlite:///{TESTS_FOLDER.joinpath(get_db_filename())}"
