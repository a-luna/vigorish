import os
from pathlib import Path

from vigorish.config.project_paths import CONFIG_FILE, DOTENV_FILE, SQLITE_PROD_URL
from vigorish.constants import ENV_VAR_NAMES
from vigorish.util.result import Result


class DotEnvFile:
    def __init__(self, dotenv_filepath=None):
        self.dotenv_filepath = dotenv_filepath
        if not self.dotenv_filepath:
            self.dotenv_filepath = Path(os.environ.get("DOTENV_FILE", DOTENV_FILE))
        self.read_dotenv_file()

    def read_dotenv_file(self):
        """Parse .env file to a dictionary of environment variables."""
        if not self.dotenv_filepath.exists():
            self.create_dotenv_file(
                config_file=os.environ.get("CONFIG_FILE", CONFIG_FILE),
                db_url=os.environ.get("DATABASE_URL", SQLITE_PROD_URL),
            )
        if not self.dotenv_filepath.is_file():
            raise TypeError(f"Unable to open file: {self.dotenv_filepath}")
        file_text = self.dotenv_filepath.read_text()
        env_var_split = [f for f in file_text.split("\n") if f and "=" in f]
        env_var_split = [s.split("=") for s in env_var_split]
        self.env_var_dict = {v[0]: v[1].strip('"').strip("'").strip() for v in env_var_split if len(v) == 2}
        if "CONFIG_FILE" not in self.env_var_dict:
            self.env_var_dict["CONFIG_FILE"] = CONFIG_FILE
        if "DATABASE_URL" not in self.env_var_dict:
            self.env_var_dict["DATABASE_URL"] = SQLITE_PROD_URL
        self.update_environment_variables()

    def create_dotenv_file(self, config_file, db_url):
        self.env_var_dict = {var_name: "" for var_name in ENV_VAR_NAMES}
        self.env_var_dict["CONFIG_FILE"] = config_file
        self.env_var_dict["DATABASE_URL"] = db_url
        self.write_dotenv_file()

    def update_environment_variables(self):
        for var_name, value in self.env_var_dict.items():
            os.environ[var_name] = value

    def get_current_value(self, env_var_name):
        if env_var_name not in ENV_VAR_NAMES:
            return None
        return self.env_var_dict.get(env_var_name, None)

    def change_value(self, env_var_name, new_value):
        if env_var_name not in ENV_VAR_NAMES:
            return Result.Fail(f"{env_var_name} is not a recognized environment variable.")
        self.env_var_dict[env_var_name] = new_value
        self.write_dotenv_file()
        self.read_dotenv_file()
        return Result.Ok()

    def restart_required_on_change(self, setting_name):
        return setting_name in ["CONFIG_FILE", "DATABASE_URL"]

    def write_dotenv_file(self):
        env_var_strings = [f"{name}={value}" for name, value in self.env_var_dict.items()]
        self.dotenv_filepath.write_text("\n".join(env_var_strings))
