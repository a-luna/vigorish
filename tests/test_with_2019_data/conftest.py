import pytest

from tests.conftest import CSV_FOLDER, JSON_FOLDER, TESTS_FOLDER
from tests.util import seed_database_with_2019_test_data
from vigorish.app import Vigorish


@pytest.fixture(scope="package")
def vig_app(request):
    """Returns an instance of the application configured to use the test DB and test config file."""
    app = Vigorish()

    def fin():
        app.db_session.close()
        for file in TESTS_FOLDER.glob("vig_*.db"):
            file.unlink()

    request.addfinalizer(fin)
    return app


@pytest.fixture(scope="package", autouse=True)
def create_test_data(vig_app):
    """Initialize DB with data to verify test functions in test_with_empty_database package."""
    vig_app.initialize_database(csv_folder=CSV_FOLDER, json_folder=JSON_FOLDER)
    assert vig_app.db_setup_complete
    seed_database_with_2019_test_data(vig_app)
    return True
