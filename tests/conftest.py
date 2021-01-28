"""Global pytest fixtures."""
from pathlib import Path


TESTS_FOLDER = Path(__file__).parent
DOTENV_FILE = TESTS_FOLDER.joinpath(".env")
CONFIG_FILE = TESTS_FOLDER.joinpath("vig.config.json")
CSV_FOLDER = TESTS_FOLDER.joinpath("csv")
BACKUP_FOLDER = TESTS_FOLDER.joinpath("backup")
DB_FILE = TESTS_FOLDER.joinpath("vig_test.db")
SQLITE_URL = f"sqlite:///{DB_FILE}"
