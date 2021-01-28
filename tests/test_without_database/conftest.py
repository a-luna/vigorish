import os
import pytest

from tests.conftest import CONFIG_FILE, DOTENV_FILE, SQLITE_URL


@pytest.fixture(autouse=True)
def env_vars(request):
    """Sets environment variables to use .env and config.json files."""
    os.environ["ENV"] = "TEST"
    os.environ["DOTENV_FILE"] = str(DOTENV_FILE)
    os.environ["CONFIG_FILE"] = str(CONFIG_FILE)
    os.environ["DATABASE_URL"] = SQLITE_URL
    return True
