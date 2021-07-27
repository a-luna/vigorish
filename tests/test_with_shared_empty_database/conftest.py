import pytest

from tests.conftest import CSV_FOLDER, JSON_FOLDER, TESTS_FOLDER
from vigorish.app import Vigorish


@pytest.fixture(scope="package")
def vig_app(request):
    """Returns an instance of the application configured to use the test DB and test config file."""
    app = Vigorish()
    app.initialize_database(csv_folder=CSV_FOLDER, json_folder=JSON_FOLDER)
    assert app.db_setup_complete

    def fin():
        app.db_session.close()
        for file in TESTS_FOLDER.glob("vig_*.db"):
            file.unlink()

    request.addfinalizer(fin)
    return app
