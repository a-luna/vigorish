from pathlib import Path

from dotenv import load_dotenv

from vigorish.config.database import SQLITE_PROD_URL
from vigorish.constants import ENV_VAR_NAMES
from vigorish.util.result import Result

VIG_FOLDER = Path.home() / ".vig"
DOTENV_FILE = VIG_FOLDER / ".env"
DEFAULT_CONFIG = VIG_FOLDER / "vig.config.json"


class DotEnvFile:
    def __init__(self):
        self.env_var_dict = self.read_dotenv_file()

    def read_dotenv_file(self):
        """Parse .env file to a dictionary of environment variables."""
        if not DOTENV_FILE.exists():
            self.create_default_dotenv_file()
        if not DOTENV_FILE.is_file():
            raise TypeError(f"Unable to open file: {DOTENV_FILE}")
        load_dotenv(DOTENV_FILE)
        file_text = DOTENV_FILE.read_text()
        env_var_split = [f for f in file_text.split("\n") if f and "=" in f]
        env_var_split = [s.split("=") for s in env_var_split]
        return {v[0]: v[1].strip('"').strip("'").strip() for v in env_var_split if len(v) == 2}

    def create_default_dotenv_file(self):
        self.env_var_dict = {var_name: "" for var_name in ENV_VAR_NAMES}
        self.change_value("CONFIG_FILE", DEFAULT_CONFIG)
        self.change_value("DATABASE_URL", SQLITE_PROD_URL)
        self.write_dotenv_file()

    def get_current_value(self, env_var_name):
        if env_var_name not in ENV_VAR_NAMES:
            return None
        return self.env_var_dict.get(env_var_name, None)

    def change_value(self, env_var_name, new_value):
        if env_var_name not in ENV_VAR_NAMES:
            return Result.Fail(f"{env_var_name} is not a recognized environment variable.")
        self.env_var_dict[env_var_name] = new_value
        self.write_dotenv_file()
        self.env_var_dict = self.read_dotenv_file()
        return Result.Ok()

    def write_dotenv_file(self):
        env_var_strings = [f"{name}={value}" for name, value in self.env_var_dict.items()]
        DOTENV_FILE.write_text("\n".join(env_var_strings))
