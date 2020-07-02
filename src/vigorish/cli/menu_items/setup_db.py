"""Menu item that allows the user to initialize/reset the database."""
import subprocess
import time

from getch import pause

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
        result = prompt_user_yes_no("Would you like to continue?")
        did_confirm_yes = result.value
        if not did_confirm_yes:
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
        result = prompt_user_yes_no(UPDATE_PROMPT)
        yes_update = result.value
        if not yes_update:
            return Result.Ok(self.exit_menu)

        for year in all_mlb_seasons:
            subprocess.run(["clear"])
            result = update_all_data_sets(self.scraped_data, self.db_session, year, True)
            time.sleep(2)
        return Result.Ok(self.exit_menu)
