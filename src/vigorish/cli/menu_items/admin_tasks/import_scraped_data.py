"""Update bbref_player_id_map.json file."""
from datetime import datetime
import subprocess
from collections import defaultdict

from getch import pause
from halo import Halo

from vigorish.cli.components import (
    get_random_cli_color,
    get_random_dots_spinner,
    multi_season_prompt,
    print_heading,
    print_message,
    user_options_prompt,
    yes_no_prompt,
)
from vigorish.cli.menu_item import MenuItem
from vigorish.constants import DATA_SET_TO_NAME_MAP, EMOJIS, MENU_NUMBERS
from vigorish.tasks.import_scraped_data import ImportScrapedDataTask
from vigorish.util.datetime_util import format_timedelta_str
from vigorish.util.result import Result

IMPORT_DATA_MESSAGE = (
    "This task uses the value of the JSON_LOCAL_FOLDER_PATH config setting to locate scraped "
    "data in the local file system from all data sets and all MLB seasons. After the search is "
    "complete, the scraped data is used to update the database."
)
IMPORT_DATA_PROMPT = "\nSelect YES to run this task\nSelect NO to return to the previous menu"
OVERWRITE_DATA_MESSAGE = (
    'By default, data that is already considered "scraped" within the database will not be '
    "updated with the data found in the local file system."
)

IMPORT_ALL_SEASONS_PROMPT = "\nSelect YES to import data for all seasons\nSelect NO to choose each season(s) to import"

OVERWRITE_DATA_PROMPT = (
    "Select KEEP EXISTING DATA to use the default behavior and only update data which is not "
    'already marked as "scraped" in the database\n'
    "Select OVERWRITE EXISTING DATA to update all data in the database with the values found in "
    "the local file system"
)


class ImportScrapedData(MenuItem):
    def __init__(self, app):
        super().__init__(app)
        self.import_scraped_data = ImportScrapedDataTask(self.app)
        self.spinners = {}
        self.menu_item_text = "Import Scraped Data from Local Folders"
        self.menu_item_emoji = EMOJIS.get("HONEY_POT")

    def launch(self, import_seasons=None, no_prompts=False, overwrite=False):
        self.no_prompts = no_prompts
        return (
            self.import_scraped_data_no_prompts(import_seasons, overwrite)
            if no_prompts
            else self.import_scraped_data_prompts(import_seasons)
        )

    def import_scraped_data_prompts(self, import_seasons):
        if not self.prompt_user_import_data():
            return Result.Ok(True)
        self.subscribe_to_events()
        if not self.prompt_user_import_all_seasons():
            import_seasons = self.prompt_user_select_seasons()
        result = self.prompt_user_overwrite_data()
        if result.failure:
            return Result.Ok(True)
        overwrite_existing_data = result.value == "OVERWRITE"
        start = datetime.now()
        result = self.import_scraped_data.execute(import_seasons, overwrite_existing_data)
        elapsed = datetime.now() - start
        self.unsubscribe_from_events()
        if result.failure:
            self.display_error_messages(result.error)
        else:
            self.display_task_duration(elapsed)
        return Result.Ok(True)

    def import_scraped_data_no_prompts(self, import_seasons, overwrite):
        self.subscribe_to_events()
        result = self.import_scraped_data.execute(import_seasons, overwrite)
        self.unsubscribe_from_events()
        if result.failure:
            self.update_menu_heading("Error!")
            print_message(result.error)
        return Result.Ok(True)

    def update_menu_heading(self, current_action):
        new_heading = (
            f"Import Scraped Data: {current_action}"
            if self.app.db_setup_complete
            else f"Setup Database: {current_action}"
        )
        subprocess.run(["clear"])
        print_heading(new_heading, fg="bright_yellow")

    def prompt_user_import_data(self):
        self.update_menu_heading("Run Task?")
        print_message(IMPORT_DATA_MESSAGE, fg="bright_yellow", bold=True)
        return yes_no_prompt(IMPORT_DATA_PROMPT, wrap=False)

    def prompt_user_import_all_seasons(self):
        self.update_menu_heading("Options")
        print_message("Would you like to import data for all seasons?", fg="bright_yellow", bold=True)
        return yes_no_prompt(IMPORT_ALL_SEASONS_PROMPT, wrap=False)

    def prompt_user_select_seasons(self):
        return multi_season_prompt(self.db_session, heading="Select Seasons to Import")

    def prompt_user_overwrite_data(self):
        self.update_menu_heading("Overwrite Existing Data?")
        print_message(OVERWRITE_DATA_MESSAGE, fg="bright_yellow", bold=True)
        choices = {
            f"{MENU_NUMBERS.get(1)}  KEEP EXISTING DATA": "KEEP",
            f"{MENU_NUMBERS.get(2)}  OVERWRITE EXISTING DATA": "OVERWRITE",
            f"{EMOJIS.get('BACK')} Return to Admin/Tasks Menu": None,
        }
        return user_options_prompt(choices, OVERWRITE_DATA_PROMPT, clear_screen=False)

    def display_error_messages(self, error_message):
        self.update_menu_heading("Error!")
        print_message(error_message)
        pause(message="\nPress any key to continue...")

    def display_task_duration(self, elapsed):
        self.update_menu_heading("Finished!")
        print_message(f"Execution time for this import task: {format_timedelta_str(elapsed)}")
        pause(message="\nPress any key to continue...")

    def error_occurred(self, error_message, data_set, year):
        self.update_menu_heading("Error!")
        error_message = f"Error occurred while updating {DATA_SET_TO_NAME_MAP[data_set]} for MLB {year}"
        self.spinners[year][data_set].fail(error_message)

    def search_local_files_start(self):
        self.update_menu_heading("In Progress...")
        self.spinners["default"] = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        self.spinners["default"].text = "Searching local folder for scraped data..."
        self.spinners["default"].start()

    def import_scraped_data_start(self):
        self.spinners["default"].stop()
        self.update_menu_heading("In Progress...")

    def import_scraped_data_for_year_start(self, year):
        self.update_menu_heading("In Progress...")
        self.spinners[year] = defaultdict(lambda: Halo())

    def import_scraped_data_set_start(self, data_set, year):
        spinner = self.spinners[year][data_set]
        spinner.spinner = get_random_dots_spinner()
        spinner.color = get_random_cli_color()
        spinner.text = f"Updating {DATA_SET_TO_NAME_MAP[data_set]} for MLB {year}..."
        spinner.start()

    def import_scraped_data_set_complete(self, data_set, year):
        import_complete = f"Successfully updated {DATA_SET_TO_NAME_MAP[data_set]} for MLB {year}!"
        self.spinners[year][data_set].succeed(import_complete)

    def import_combined_game_data_start(self, year):
        self.spinners["combined_data"] = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        self.spinners["combined_data"].text = f"Updating combined game data for MLB {year}..."
        self.spinners["combined_data"].start()

    def import_combined_game_data_complete(self, year):
        import_complete = f"Successfully updated combined game data for MLB {year}!"
        if "combined_data" not in self.spinners:
            self.spinners["combined_data"] = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
            self.spinners["combined_data"].text = f"Updating combined game data for MLB {year}..."
            self.spinners["combined_data"].start()
        self.spinners["combined_data"].succeed(import_complete)

    def import_scraped_data_complete(self):
        self.update_menu_heading("Complete!")
        print_message("Successfully imported all scraped data from local files", fg="bright_yellow", bold=True)
        if not self.no_prompts:
            pause(message="\nPress any key to continue...")

    def subscribe_to_events(self):
        self.import_scraped_data.events.error_occurred += self.error_occurred
        self.import_scraped_data.events.search_local_files_start += self.search_local_files_start
        self.import_scraped_data.events.import_scraped_data_start += self.import_scraped_data_start
        self.import_scraped_data.events.import_scraped_data_complete += self.import_scraped_data_complete
        self.import_scraped_data.events.import_scraped_data_for_year_start += self.import_scraped_data_for_year_start
        self.import_scraped_data.events.import_scraped_data_set_start += self.import_scraped_data_set_start
        self.import_scraped_data.events.import_scraped_data_set_complete += self.import_scraped_data_set_complete
        self.import_scraped_data.events.import_combined_game_data_start += self.import_combined_game_data_start
        self.import_scraped_data.events.import_combined_game_data_complete += self.import_combined_game_data_complete

    def unsubscribe_from_events(self):
        self.import_scraped_data.events.error_occurred -= self.error_occurred
        self.import_scraped_data.events.search_local_files_start -= self.search_local_files_start
        self.import_scraped_data.events.import_scraped_data_start -= self.import_scraped_data_start
        self.import_scraped_data.events.import_scraped_data_complete -= self.import_scraped_data_complete
        self.import_scraped_data.events.import_scraped_data_for_year_start -= self.import_scraped_data_for_year_start
        self.import_scraped_data.events.import_scraped_data_set_start -= self.import_scraped_data_set_start
        self.import_scraped_data.events.import_scraped_data_set_complete -= self.import_scraped_data_set_complete
        self.import_scraped_data.events.import_combined_game_data_start -= self.import_combined_game_data_start
        self.import_scraped_data.events.import_combined_game_data_complete -= self.import_combined_game_data_complete
