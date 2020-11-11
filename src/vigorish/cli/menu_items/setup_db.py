"""Menu item that allows the user to initialize/reset the database."""
import subprocess
import time
from datetime import datetime
from pathlib import Path

from getch import pause

from vigorish.cli.components.prompts import yes_no_prompt
from vigorish.cli.components.util import print_heading, print_message, shutdown_cli_immediately
from vigorish.cli.menu_item import MenuItem
from vigorish.cli.menu_items.admin_tasks.update_player_id_map import UpdatePlayerIdMap
from vigorish.config.database import (
    db_setup_complete,
    initialize_database,
    reset_database_connection,
    Season,
)
from vigorish.constants import EMOJI_DICT
from vigorish.enums import DataSet
from vigorish.tasks.import_scraped_data import ImportScrapedDataTask
from vigorish.util.result import Result

SETUP_HEADING = (
    "Before you can begin scraping data, you must initialize the database with initial player, "
    "team and season data."
)
SETUP_MESSAGE = "Select YES to initialize the database\nSelect NO to return to the previous menu"
RESET_MESSAGE = "Would you like to reset the database with initial player, team and season data?"
WARNING = (
    "WARNING! All existing data will be deleted if you choose to reset the database. This "
    "action cannot be undone."
)
IMPORT_SCRAPED_DATA_MESSAGE = (
    "Would you like to update the database using files from previous vigorish installations? "
    "Files from any MLB season and any data set found in the location specified by the "
    "JSON_LOCAL_FOLDER_PATH config setting will be used for this step.\n"
)
IMPORT_SCRAPED_DATA_PROMPT = (
    "Select YES to import all data in the location above\n"
    "Select NO to return to the previous menu"
)
DB_INITIALIZED = "Database has been successfully initialized.\n"


class SetupDatabase(MenuItem):
    def __init__(self, app):
        super().__init__(app)
        self.update_id_map_task = UpdatePlayerIdMap(self.app)
        self.import_data_task = ImportScrapedDataTask(self.app)
        self.db_initialized = db_setup_complete(self.db_engine, self.db_session)
        self.menu_item_text = "Reset Database" if self.db_initialized else "Setup Database"
        self.menu_item_emoji = EMOJI_DICT["BOMB"] if self.db_initialized else EMOJI_DICT["DIZZY"]

    def launch(self):
        restart_required = self.db_initialized
        if not self.prompt_user_run_task():
            return Result.Ok(self.exit_menu)
        return (
            self.update_player_id_map()
            .on_success(self.update_database_connection)
            .on_success(self.create_and_populate_database_tables)
            .on_success(self.import_scraped_data, restart_required)
        )

    def prompt_user_run_task(self):
        if self.db_initialized:
            message = WARNING
            message_color = "bright_red"
            prompt = RESET_MESSAGE
        else:
            message = SETUP_HEADING
            message_color = None
            prompt = SETUP_MESSAGE
        subprocess.run(["clear"])
        print_heading(self.get_heading("Run Task?"), fg="bright_yellow")
        print_message(message, fg=message_color)
        return yes_no_prompt(prompt, wrap=False)

    def get_heading(self, current_action):
        return (
            f"Reset Database: {current_action}"
            if self.db_initialized
            else f"Setup Database: {current_action}"
        )

    def update_player_id_map(self):
        if not self.db_initialized:
            return Result.Ok()
        result = Season.is_date_in_season(self.db_session, datetime.now())
        if result.failure:
            return Result.Ok()
        subprocess.run(["clear"])
        return self.update_id_map_task.launch()

    def update_database_connection(self):
        if not self.db_initialized:
            return Result.Ok()
        result = reset_database_connection(self.app)
        if result.failure:
            return result
        self.app = result.value
        return Result.Ok()

    def create_and_populate_database_tables(self):
        subprocess.run(["clear"])
        print_heading(self.get_heading("In Progress"), fg="bright_yellow")
        result = initialize_database(self.app)
        if result.success:
            subprocess.run(["clear"])
            print_heading(self.get_heading("Complete"), fg="bright_yellow")
            print_message(DB_INITIALIZED, fg="bright_green", bold=True)
            pause(message="Press any key to continue...")
        return result

    def import_scraped_data(self, restart_required):
        if not self.import_scraped_data_prompt():
            return self.setup_complete(restart_required)
        result = self.import_data_task.execute(overwrite_existing=True)
        if result.error:
            print_message(result.error, fg="bright_red")
            pause(message="Press any key to continue...")
        else:
            time.sleep(2)
        return self.setup_complete(restart_required)

    def import_scraped_data_prompt(self):
        local_folder_path = self.config.all_settings.get("JSON_LOCAL_FOLDER_PATH")
        example_folder = local_folder_path.current_setting(data_set=DataSet.BBREF_BOXSCORES)
        root_folder = Path(example_folder.resolve(year=2019)).parent.parent
        current_setting = f"JSON_LOCAL_FOLDER_PATH: {root_folder}"
        subprocess.run(["clear"])
        print_heading(self.get_heading("Import Local JSON Folder?"), fg="bright_yellow")
        print_message(IMPORT_SCRAPED_DATA_MESSAGE)
        print_message(current_setting, wrap=False)
        return yes_no_prompt(IMPORT_SCRAPED_DATA_PROMPT, wrap=False)

    def setup_complete(self, restart_required):
        if restart_required:
            shutdown_cli_immediately()
        return Result.Ok(self.exit_menu)
