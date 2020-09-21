"""Menu item that allows the user to initialize/reset the database."""
import subprocess
import time

from getch import pause
from sqlalchemy.orm import sessionmaker

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.menu_items.admin_tasks.update_player_id_map import UpdatePlayerIdMap
from vigorish.cli.prompts import prompt_user_yes_no
from vigorish.cli.util import print_message
from vigorish.config.database import initialize_database, db_setup_complete
from vigorish.constants import EMOJI_DICT
from vigorish.tasks.import_scraped_data import ImportScrapedDataInLocalFolder
from vigorish.util.result import Result

SETUP_HEADER = (
    "Before you can begin scraping data, you must initialize the database with initial player, "
    "team and season data.\n"
)
SETUP_MESSAGE = (
    "Select YES to initialize the database\n" "Select NO to return to the previous menu"
)
RESET_MESSAGE = "Would you like to reset the database with initial player, team and season data?"
WARNING = (
    "WARNING! All existing data will be deleted if you choose to reset the database. This "
    "action cannot be undone.\n"
)
DB_INITIALIZED = "\nDatabase has been successfully initialized."
RESTART_WARNING = "\nApplication must be restarted for these changes to take effect!"
UPDATE_PROMPT = (
    "Would you like to update the database based on files that currently exist in the local file "
    "system? (This relies on the value of the JSON_LOCAL_FOLDER_PATH setting in the config file)"
)
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
            print_message(WARNING, fg="bright_red", bold=True)
            yes = prompt_user_yes_no("Would you like to continue?")
        else:
            print_message(SETUP_HEADER, fg="bright_yellow", bold=True)
            yes = prompt_user_yes_no(SETUP_MESSAGE, wrap=False)
        if not yes:
            return Result.Ok(self.exit_menu)

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
        if not prompt_user_yes_no(UPDATE_PROMPT):
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

    def setup_complete(self, restart_required):
        if not restart_required:
            return Result.Ok(self.exit_menu)
        print_message(RESTART_WARNING, fg="bright_magenta", bold=True)
        print_message(SHUTDOWN_MESSAGE, fg="bright_magenta", bold=True)
        pause(message="Press any key to continue...")
        exit(0)
