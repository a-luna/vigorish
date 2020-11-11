"""Update bbref_player_id_map.json file."""
import subprocess

from halo import Halo

from vigorish.cli.components import (
    get_random_cli_color,
    get_random_dots_spinner,
    print_message,
    user_options_prompt,
    yes_no_prompt,
)
from vigorish.cli.menu_item import MenuItem
from vigorish.constants import EMOJI_DICT, MENU_NUMBERS
from vigorish.tasks.import_scraped_data import ImportScrapedDataTask
from vigorish.util.result import Result

IMPORT_DATA_MESSAGE = (
    "This task uses the value of the JSON_LOCAL_FOLDER_PATH config setting to locate scraped "
    "data in the local file system from all data sets and all MLB seasons. After the search is "
    "complete, the scraped data is used to update the database."
)
IMPORT_DATA_PROMPT = "Select YES to run this task\nSelect NO to return to the previous menu"
OVERWRITE_DATA_MESSAGE = (
    'By default, data that is already considered "scraped" within the database will not be '
    "updated with the data found in the local file system."
)
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
        self.spinner = Halo(color=get_random_cli_color(), spinner=get_random_dots_spinner())
        self.menu_item_text = "Import Scraped Data from Local Folders"
        self.menu_item_emoji = EMOJI_DICT.get("HONEY_POT")

    def launch(self):
        if not self.prompt_user_import_data():
            return Result.Ok(True)
        self.subscribe_to_events()
        result = self.prompt_user_overwrite_data()
        if result.failure:
            return Result.Ok(True)
        overwrite_existing_data = True if result.value == "OVERWRITE" else False
        result = self.import_scraped_data.execute(overwrite_existing_data)
        self.unsubscribe_from_events()
        if result.failure:
            return result
        return Result.Ok(True)

    def prompt_user_import_data(self):
        subprocess.run(["clear"])
        print_message(IMPORT_DATA_MESSAGE, fg="bright_yellow", bold=True)
        return yes_no_prompt(IMPORT_DATA_PROMPT, wrap=False)

    def prompt_user_overwrite_data(self):
        subprocess.run(["clear"])
        print_message(OVERWRITE_DATA_MESSAGE, fg="bright_yellow", bold=True)
        choices = {
            f"{MENU_NUMBERS.get(1)}  KEEP EXISTING DATA": "KEEP",
            f"{MENU_NUMBERS.get(2)}  OVERWRITE EXISTING DATA": "OVERWRITE",
            f"{EMOJI_DICT.get('BACK')} Return to Admin/Tasks Menu": None,
        }
        return user_options_prompt(choices, OVERWRITE_DATA_PROMPT, clear_screen=False)

    def error_occurred(self, error_message, data_set, year):
        self.spinner.fail(f"Error occurred while updating {data_set} for MLB {year}")

    def subscribe_to_events(self):
        self.import_scraped_data.events.error_occurred += self.error_occurred

    def unsubscribe_from_events(self):
        self.import_scraped_data.events.error_occurred -= self.error_occurred
