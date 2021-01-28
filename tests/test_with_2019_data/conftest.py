import os
import pytest

from tests.conftest import CONFIG_FILE, CSV_FOLDER, DOTENV_FILE, SQLITE_URL
from tests.util import seed_database_with_2019_test_data
from vigorish.app import Vigorish


@pytest.fixture(autouse=True)
def env_vars(request):
    """Sets environment variables to use .env and config.json files."""
    os.environ["ENV"] = "TEST"
    os.environ["DOTENV_FILE"] = str(DOTENV_FILE)
    os.environ["CONFIG_FILE"] = str(CONFIG_FILE)
    os.environ["DATABASE_URL"] = SQLITE_URL
    return True


@pytest.fixture(scope="package")
def vig_app(request):
    """Returns an instance of the application configured to use the test DB and test config file."""
    app = Vigorish()

    def fin():
        app.db_session.close()

    request.addfinalizer(fin)
    return app


@pytest.fixture(scope="package", autouse=True)
def create_test_data(vig_app):
    """Initialize DB with data to verify test functions in test_with_empty_database package."""
    vig_app.initialize_database(csv_folder=CSV_FOLDER)
    assert vig_app.db_setup_complete
    seed_database_with_2019_test_data(vig_app)
    return True
