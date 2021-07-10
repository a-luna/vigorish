"""Menu item that allows the user to initialize/reset the database."""
import subprocess
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from getch import pause
from halo import Halo

import vigorish.database as db
from vigorish.cli.components.prompts import yes_no_prompt
from vigorish.cli.components.util import (
    get_random_cli_color,
    get_random_dots_spinner,
    print_error,
    print_message,
    shutdown_cli_immediately,
)
from vigorish.cli.menu_item import MenuItem
from vigorish.cli.menu_items.admin_tasks.update_player_id_map import UpdatePlayerIdMap
from vigorish.constants import EMOJIS
from vigorish.enums import DataSet
from vigorish.tasks.import_scraped_data import ImportScrapedDataTask
from vigorish.util.result import Result

SETUP_HEADING = (
    "Before you can begin scraping data, you must initialize the database with initial player, team and season data."
)
SETUP_MESSAGE = "\nSelect YES to initialize the database\nSelect NO to return to the previous menu"
RESET_MESSAGE = "\nWould you like to reset the database with initial player, team and season data?"
WARNING = (
    "WARNING! All existing data will be deleted if you choose to reset the database. This action cannot be undone."
)
IMPORT_SCRAPED_DATA_MESSAGE = (
    "Would you like to update the database using files from previous vigorish installations? "
    "Files from any MLB season and any data set found in the location specified by the "
    "JSON_LOCAL_FOLDER_PATH config setting will be used for this step.\n"
)
IMPORT_SCRAPED_DATA_PROMPT = (
    "\nSelect YES to import all data in the location above\n" "Select NO to return to the previous menu"
)
DB_INITIALIZED = "Database has been successfully initialized.\n"


class SetupDatabase(MenuItem):
    def __init__(self, app):
        super().__init__(app)
        self.update_id_map_task = UpdatePlayerIdMap(self.app)
        self.import_data_task = ImportScrapedDataTask(self.app)
        self.db_initialized = self.app.db_setup_complete
        self.spinners = {}
        self.menu_item_text = "Reset Database" if self.db_initialized else "Setup Database"
        self.menu_item_emoji = EMOJIS["BOMB"] if self.db_initialized else EMOJIS["DIZZY"]
        self.menu_heading = self._menu_item_text

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
        self.update_menu_heading("Run Task?")
        print_message(message, fg=message_color)
        return yes_no_prompt(prompt, wrap=False)

    def update_player_id_map(self):
        if not self.db_initialized:
            return Result.Ok()
        result = db.Season.is_date_in_season(self.db_session, datetime.now())
        if result.failure:
            return Result.Ok()
        subprocess.run(["clear"])
        return self.update_id_map_task.launch(no_prompts=True)

    def update_database_connection(self):
        if self.db_initialized:
            self.app.reset_database_connection()
        return Result.Ok()

    def create_and_populate_database_tables(self):
        self.update_menu_heading("In Progress")
        result = self.app.initialize_database()
        if result.success:
            self.update_menu_heading("Complete!", heading_color="bright_green")
            print_message(DB_INITIALIZED, fg="bright_green", bold=True)
            pause(message="Press any key to continue...")
        return result

    def import_scraped_data(self, restart_required):
        if not self.import_scraped_data_prompt():
            return self.setup_complete(restart_required)
        subprocess.run(["clear"])
        self.subscribe_to_events()
        result = self.import_data_task.execute(overwrite_existing=True)
        if result.error:
            print_message(result.error, fg="bright_red")
            pause(message="Press any key to continue...")
        else:
            time.sleep(2)
        self.unsubscribe_from_events()
        return self.setup_complete(restart_required)

    def import_scraped_data_prompt(self):
        example_folder = self.app.get_current_setting("JSON_LOCAL_FOLDER_PATH", DataSet.BBREF_BOXSCORES, year=2019)
        root_folder = Path(example_folder).parent.parent
        current_setting = f"JSON_LOCAL_FOLDER_PATH: {root_folder}"
        self.update_menu_heading("Import Local JSON Folder?")
        print_message(IMPORT_SCRAPED_DATA_MESSAGE)
        print_message(current_setting, wrap=False)
        return yes_no_prompt(IMPORT_SCRAPED_DATA_PROMPT, wrap=False)

    def setup_complete(self, restart_required):
        if restart_required:
            shutdown_cli_immediately()
        return Result.Ok(self.exit_menu)

    def error_occurred(self, error_message, data_set, year):
        self.update_menu_heading("Error!", heading_color="bright_red")
        self.spinner.fail(f"Error occurred while updating {data_set} for MLB {year}")
        print_error(f"\nError: {error_message}")

    def search_local_files_start(self):
        self.update_menu_heading("In Progress")
        self.spinners["default"] = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        self.spinners["default"].text = "Searching local folder for scraped data..."
        self.spinners["default"].start()

    def import_scraped_data_start(self):
        self.spinners["default"].stop()
        self.update_menu_heading("In Progress...")

    def import_scraped_data_complete(self):
        self.update_menu_heading("Complete!", heading_color="bright_green")
        success = "Successfully imported all scraped data from local files"
        print_message(success, fg="bright_yellow", bold=True)
        pause(message="\nPress any key to continue...")

    def import_scraped_data_for_year_start(self, year):
        self.update_menu_heading("In Progress...")
        self.spinners[year] = defaultdict(lambda: Halo())

    def import_scraped_data_set_start(self, data_set, year):
        spinner = self.spinners[year][data_set]
        spinner.spinner = get_random_dots_spinner()
        spinner.color = get_random_cli_color()
        spinner.text = f"Updating {data_set} for MLB {year}..."
        spinner.start()

    def import_scraped_data_set_complete(self, data_set, year):
        self.spinners[year][data_set].succeed(f"Successfully updated {data_set} for MLB {year}!")

    def subscribe_to_events(self):
        self.import_data_task.events.error_occurred += self.error_occurred
        self.import_data_task.events.search_local_files_start += self.search_local_files_start
        self.import_data_task.events.import_scraped_data_start += self.import_scraped_data_start
        self.import_data_task.events.import_scraped_data_complete += self.import_scraped_data_complete
        self.import_data_task.events.import_scraped_data_for_year_start += self.import_scraped_data_for_year_start
        self.import_data_task.events.import_scraped_data_set_start += self.import_scraped_data_set_start
        self.import_data_task.events.import_scraped_data_set_complete += self.import_scraped_data_set_complete

    def unsubscribe_from_events(self):
        self.import_data_task.events.error_occurred -= self.error_occurred
        self.import_data_task.events.search_local_files_start -= self.search_local_files_start
        self.import_data_task.events.import_scraped_data_start -= self.import_scraped_data_start
        self.import_data_task.events.import_scraped_data_complete -= self.import_scraped_data_complete
        self.import_data_task.events.import_scraped_data_for_year_start -= self.import_scraped_data_for_year_start
        self.import_data_task.events.import_scraped_data_set_start -= self.import_scraped_data_set_start
        self.import_data_task.events.import_scraped_data_set_complete -= self.import_scraped_data_set_complete
