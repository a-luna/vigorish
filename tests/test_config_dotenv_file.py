import os

from tests.conftest import TESTS_FOLDER, SQLITE_URL, CONFIG_FILE
from vigorish.config.dotenv_file import DotEnvFile


def test_dotenv_file():
    default_dotenv = TESTS_FOLDER.joinpath(".env.default")
    if default_dotenv.exists():
        default_dotenv.unlink()
    os.environ["DOTENV_FILE"] = str(default_dotenv)
    dotenv = DotEnvFile()
    check_db_url = dotenv.get_current_value("DATABASE_URL")
    assert check_db_url == SQLITE_URL
    config_file_before = dotenv.get_current_value("CONFIG_FILE")
    new_config_file = TESTS_FOLDER.joinpath("new.config.json")
    result = dotenv.change_value("CONFIG_FILE", new_config_file)
    assert result.success
    config_file_after = dotenv.get_current_value("CONFIG_FILE")
    assert config_file_before == str(CONFIG_FILE)
    assert config_file_after == str(new_config_file)

    invalid_var_name = dotenv.get_current_value("SERVER_IP")
    assert not invalid_var_name
    result = dotenv.change_value("SERVER_IP", "192.168.1.1")
    assert result.failure
    assert "SERVER_IP is not a recognized environment variable." in result.error

    assert not dotenv.restart_required_on_change("AWS_DEFAULT_REGION")
    assert dotenv.restart_required_on_change("DATABASE_URL")
    default_dotenv.unlink()
