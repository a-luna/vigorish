"""Update bbref_player_id_map.json file."""
from halo import Halo

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.util import get_random_cli_color, get_random_dots_spinner
from vigorish.constants import EMOJI_DICT
from vigorish.tasks.import_scraped_data import ImportScrapedDataInLocalFolder
from vigorish.util.result import Result


class ImportScrapedDataTask(MenuItem):
    def __init__(self, app):
        super().__init__(app)
        self.import_scraped_data = ImportScrapedDataInLocalFolder(self.app)
        self.spinner = Halo(color=get_random_cli_color(), spinner=get_random_dots_spinner())
        self.menu_item_text = "Import Scraped Data from Local Folders"
        self.menu_item_emoji = EMOJI_DICT.get("HONEY_POT")

    def launch(self):
        result = self.import_scraped_data.execute(overwrite_existing=True)
        if result.failure:
            return result
        return Result.Ok(True)

    def error_occurred(self, error_message, data_set, year):
        self.spinner.fail(f"Error occurred while updating {data_set} for MLB {year}")

    def subscribe_to_events(self):
        self.import_scraped_data.events.error_occurred += self.error_occurred
        self.import_scraped_data.events.no_scraped_data_found += self.no_scraped_data_found
        self.import_scraped_data.events.remove_invalid_pitchfx_logs_start += (
            self.remove_invalid_pitchfx_logs_start
        )
        self.import_scraped_data.events.remove_invalid_pitchfx_logs_complete += (
            self.remove_invalid_pitchfx_logs_complete
        )
        self.import_scraped_data.events.import_scraped_data_start += self.import_scraped_data_start
        self.import_scraped_data.events.import_scraped_data_complete += (
            self.import_scraped_data_complete
        )
