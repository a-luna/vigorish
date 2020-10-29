import os
from pathlib import Path

from dotenv import load_dotenv

from vigorish.config.database import SQLITE_PROD_URL
from vigorish.constants import ENV_VAR_NAMES
from vigorish.util.result import Result

VIG_FOLDER = Path.home() / ".vig"
DOTENV_FILE = VIG_FOLDER / ".env"
DEFAULT_CONFIG = VIG_FOLDER / "vig.config.json"


def create_default_dotenv_file(dotenv_file, config_file=None, db_url=None):
    env_var_dict = {var_name: "" for var_name in ENV_VAR_NAMES}
    env_var_dict["CONFIG_FILE"] = config_file if config_file else DEFAULT_CONFIG
    env_var_dict["DATABASE_URL"] = db_url if db_url else SQLITE_PROD_URL
    env_var_strings = [f"{name}={value}" for name, value in env_var_dict.items()]
    dotenv_file.write_text("\n".join(env_var_strings))


class DotEnvFile:
    def __init__(self, dotenv_filepath=None):
        if os.environ.get("ENV") == "TEST":
            self.dotenv_filepath = Path(os.environ.get("DOTENV_FILE"))
        else:
            self.dotenv_filepath = dotenv_filepath if dotenv_filepath else DOTENV_FILE
        self.env_var_dict = self.read_dotenv_file()

    def read_dotenv_file(self):
        """Parse .env file to a dictionary of environment variables."""
        if not self.dotenv_filepath.exists():
            create_default_dotenv_file(dotenv_file=self.dotenv_filepath)
        if not self.dotenv_filepath.is_file():
            raise TypeError(f"Unable to open file: {self.dotenv_filepath}")
        load_dotenv(self.dotenv_filepath)
        file_text = self.dotenv_filepath.read_text()
        env_var_split = [f for f in file_text.split("\n") if f and "=" in f]
        env_var_split = [s.split("=") for s in env_var_split]
        return {v[0]: v[1].strip('"').strip("'").strip() for v in env_var_split if len(v) == 2}

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

    def restart_required_on_change(self, setting_name):
        return setting_name in ["CONFIG_FILE", "DATABASE_URL"]

    def write_dotenv_file(self):
        env_var_strings = [f"{name}={value}" for name, value in self.env_var_dict.items()]
        self.dotenv_filepath.write_text("\n".join(env_var_strings))
