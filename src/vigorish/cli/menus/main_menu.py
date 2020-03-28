"""The main menu for the CLI."""
from vigorish.cli.menu import Menu
from vigorish.cli.menus.all_jobs_menu import AllJobsMenu
from vigorish.cli.menus.settings_menu import ConfigSettingsMenu
from vigorish.cli.menu_items.create_job import CreateJobMenuItem
from vigorish.cli.menu_items.exit_program import ExitProgramMenuItem
from vigorish.cli.menu_items.setup_db import SetupDBMenuItem
from vigorish.config.database import db_setup_complete
from vigorish.util.result import Result


class MainMenu(Menu):
    def __init__(self, app) -> None:
        self.config = app["config"]
        self.db_engine = app["engine"]
        self.db_session = app["session"]
        self.scraped_data = app["scraped_data"]
        self.menu_text = "Welcome to vigorish!"

    def populate_menu_items(self) -> None:
        self.menu_items = []
        self.menu_items.append(ConfigSettingsMenu(self.config))
        self.menu_items.append(ExitProgramMenuItem())
        if db_setup_complete(self.db_engine, self.db_session):
            self.menu_items.insert(
                0, CreateJobMenuItem(self.db_session, self.config, self.scraped_data)
            )
            self.menu_items.insert(1, AllJobsMenu(self.db_session, self.config, self.scraped_data))
            db_menu_text = "Reset Database"
            menu_index = 2
        else:
            db_menu_text = "Setup Database"
            menu_index = 0
        db_menu_item = SetupDBMenuItem(db_menu_text, self.db_engine, self.db_session)
        self.menu_items.insert(menu_index, db_menu_item)
