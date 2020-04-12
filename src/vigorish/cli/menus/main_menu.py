"""The main menu for the CLI."""
from vigorish.cli.menu import Menu
from vigorish.cli.menus.all_jobs_menu import AllJobsMenu
from vigorish.cli.menus.config_settings_menu import ConfigSettingsMenu
from vigorish.cli.menus.dotenv_settings_menu import DotEnvSettingsMenu
from vigorish.cli.menu_items.create_job import CreateJobMenuItem
from vigorish.cli.menu_items.exit_program import ExitProgramMenuItem
from vigorish.cli.menu_items.npm_install_update import NpmInstallUpdate
from vigorish.cli.menu_items.setup_db import SetupDBMenuItem
from vigorish.config.database import db_setup_complete
from vigorish.constants import EMOJI_DICT


class MainMenu(Menu):
    def __init__(self, app) -> None:
        self.dotenv = app["dotenv"]
        self.config = app["config"]
        self.db_engine = app["engine"]
        self.db_session = app["session"]
        self.scraped_data = app["scraped_data"]
        self.menu_text = "Welcome to vigorish!"
        if not node_installed() and not node_installed(exe_name="nodejs"):
            error = "Error! Node.js is not installed, see README for install instructions."
            self.menu_text += f"\n{error}"

    def populate_menu_items(self) -> None:
        self.menu_items = []
        self.menu_items.append(ConfigSettingsMenu(self.config))
        self.menu_items.append(DotEnvSettingsMenu(self.dotenv))
        self.menu_items.append(NpmInstallUpdate())
        self.menu_items.append(ExitProgramMenuItem())
        if db_setup_complete(self.db_engine, self.db_session):
            self.menu_items.insert(
                0, CreateJobMenuItem(self.db_session, self.config, self.scraped_data)
            )
            self.menu_items.insert(1, AllJobsMenu(self.db_session, self.config, self.scraped_data))
            db_text = "Reset Database"
            db_emoji = EMOJI_DICT.get("BOMB")
            db_index = 5
        else:
            db_text = "Setup Database"
            db_emoji = EMOJI_DICT.get("DIZZY")
            db_index = 0
        db_menu_item = SetupDBMenuItem(db_text, db_emoji, self.db_engine, self.db_session)
        self.menu_items.insert(db_index, db_menu_item)
