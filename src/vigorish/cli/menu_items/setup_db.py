"""Menu item that allows the user to initialize/reset the database."""
import subprocess
import time
from datetime import datetime
from pathlib import Path

from getch import pause
from sqlalchemy.orm import sessionmaker

from vigorish.cli.components import print_message, yes_no_prompt
from vigorish.cli.menu_item import MenuItem
from vigorish.cli.menu_items.admin_tasks.update_player_id_map import UpdatePlayerIdMap
from vigorish.config.database import db_setup_complete, initialize_database, Season
from vigorish.constants import EMOJI_DICT
from vigorish.enums import DataSet
from vigorish.tasks.import_scraped_data import ImportScrapedDataInLocalFolder
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
DB_INITIALIZED = "\nDatabase has been successfully initialized."
RESTART_WARNING = "\nApplication must be restarted for these changes to take effect!"
SHUTDOWN_MESSAGE = "Shutting down vigorish!"


class SetupDBMenuItem(MenuItem):
    def __init__(self, app):
        super().__init__(app)
        self.update_player_id_map = UpdatePlayerIdMap(self.app)
        self.import_scraped_data = ImportScrapedDataInLocalFolder(self.app)
        self.db_initialized = db_setup_complete(self.db_engine, self.db_session)
        self.menu_item_text = "Reset Database" if self.db_initialized else "Setup Database"
        self.menu_item_emoji = EMOJI_DICT["BOMB"] if self.db_initialized else EMOJI_DICT["DIZZY"]

    def launch(self):
        subprocess.run(["clear"])
        restart_required = self.db_initialized
        if self.db_initialized:
            print_message(WARNING, fg="bright_red")
            yes = yes_no_prompt(RESET_MESSAGE)
        else:
            print_message(SETUP_HEADING, fg="bright_yellow")
            yes = yes_no_prompt(SETUP_MESSAGE, wrap=False)
        if not yes:
            return Result.Ok(self.exit_menu)

        if self.db_initialized:
            result = Season.is_date_in_season(self.db_session, datetime.now())
            if result.success:
                subprocess.run(["clear"])
                result = self.update_player_id_map.launch()
                if result.failure:
                    return result

        subprocess.run(["clear"])
        result = initialize_database(self.app)
        if result.failure:
            return result
        print_message(DB_INITIALIZED, fg="bright_green", bold=True)
        pause(message="Press any key to continue...")

        subprocess.run(["clear"])
        if not self.import_scraped_data_prompt():
            return self.setup_complete(restart_required)

        if restart_required:
            self.db_session.close()
            session_maker = sessionmaker(bind=self.db_engine)
            self.db_session = session_maker()

        subprocess.run(["clear"])
        result = self.import_scraped_data.execute(overwrite_existing=True)
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
        print_message(IMPORT_SCRAPED_DATA_MESSAGE, fg="bright_yellow")
        print_message(current_setting, fg="bright_yellow", wrap=False)
        return yes_no_prompt(IMPORT_SCRAPED_DATA_PROMPT, wrap=False)

    def setup_complete(self, restart_required):
        if not restart_required:
            return Result.Ok(self.exit_menu)
        print_message(RESTART_WARNING, fg="bright_magenta", bold=True)
        print_message(SHUTDOWN_MESSAGE, fg="bright_magenta", bold=True)
        pause(message="Press any key to continue...")
        exit(0)
