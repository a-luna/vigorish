"""Menu item that allows the user to initialize/reset the database."""
import subprocess
import time

from getch import pause
from sqlalchemy.orm import sessionmaker

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.prompts import prompt_user_yes_no
from vigorish.cli.util import print_message
from vigorish.config.database import initialize_database, db_setup_complete, Season
from vigorish.constants import EMOJI_DICT
from vigorish.status.update_status_local_files import (
    local_folder_has_parsed_data_for_season,
    update_all_data_sets,
)
from vigorish.util.result import Result

SETUP_MESSAGE = (
    "Before you can begin scraping data, you must initialize the database with initial player, "
    "team and season data.\n"
)
RESET_MESSAGE = "Would you like to reset the database with initial player, team and season data?"
WARNING = (
    "WARNING! Before the setup process begins, all existing data will be "
    "deleted. This cannot be undone.\n"
)
DB_INITIALIZED = "\nDatabase has been successfully initialized."
UPDATE_PROMPT = (
    "Would you like to update the database based on files that currently exist in the local file "
    "system? (This relies on the value of the JSON_LOCAL_FOLDER_PATH setting in the config file)"
)
RESTART_WARNING = "\nApplication must be restarted for these changes to take effect!"


class SetupDBMenuItem(MenuItem):
    def __init__(self, app):
        super().__init__(app)
        self.db_initialized = db_setup_complete(self.db_engine, self.db_session)
        self.menu_item_text = "Reset Database" if self.db_initialized else "Setup Database"
        self.menu_item_emoji = EMOJI_DICT["BOMB"] if self.db_initialized else EMOJI_DICT["DIZZY"]

    def launch(self):
        subprocess.run(["clear"])
        if self.db_initialized:
            print_message(RESET_MESSAGE)
            print_message(WARNING, fg="bright_red", bold=True)
        else:
            print_message(SETUP_MESSAGE)
        if not prompt_user_yes_no("Would you like to continue?"):
            return Result.Ok(self.exit_menu)

        subprocess.run(["clear"])
        result = initialize_database(self.db_engine, self.db_session)
        if result.failure:
            return result
        print_message(DB_INITIALIZED, fg="bright_green", bold=True)
        pause(message="Press any key to continue...")

        all_mlb_seasons = [season.year for season in Season.all_regular_seasons(self.db_session)]
        local_folder_has_parsed_data = any(
            local_folder_has_parsed_data_for_season(self.scraped_data, year)
            for year in all_mlb_seasons
        )
        if not local_folder_has_parsed_data:
            return Result.Ok(self.exit_menu)

        subprocess.run(["clear"])
        if not prompt_user_yes_no(UPDATE_PROMPT):
            return Result.Ok(self.exit_menu)

        restart_required = self.db_initialized
        if restart_required:
            self.db_session.close()
            session_maker = sessionmaker(bind=self.db_engine)
            self.db_session = session_maker()

        for year in all_mlb_seasons:
            subprocess.run(["clear"])
            result = update_all_data_sets(self.scraped_data, self.db_session, year, True)
            if result.error:
                print_message(result.error, fg="bright_red")
                pause(message="Press any key to continue...")
            else:
                time.sleep(2)
        if not restart_required:
            return Result.Ok(self.exit_menu)
        print_message(RESTART_WARNING, fg="bright_magenta", bold=True)
        pause(message="Press any key to continue...")
        exit(0)
