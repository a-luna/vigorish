"""Menu item that returns the user to the previous menu."""
import subprocess

from halo import Halo

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.prompts import season_prompt, data_sets_prompt
from vigorish.constants import EMOJI_DICT
from vigorish.enums import DocFormat
from vigorish.util.numeric_helpers import ONE_KB
from vigorish.util.result import Result


class BulkDownloadHtmlFromS3(MenuItem):
    def __init__(self, app):
        super().__init__(app)
        self.tracker = {}
        self.download_url_ids = []
        self.menu_item_text = " Download HTML from S3"
        self.menu_item_emoji = EMOJI_DICT.get("CLOUD", "")
        self.exit_menu = False

    def launch(self):
        prompt = "Select a season to download all html files stored in S3:"
        result = season_prompt(self.db_session, prompt)
        if result.failure:
            return Result.Ok(False)
        season = result.value
        data_sets = data_sets_prompt("Select data sets to download all HTML:")
        self.download_all_html_from_s3(data_sets, season.year)

    def download_all_html_from_s3(self, data_sets, year):
        subprocess.run(["clear"])
        spinner = Halo(color="yellow", spinner="dots3")
        spinner.start()
        for data_set in data_sets:
            spinner.text = f"Preparing to download {data_set} HTML for MLB {year}..."
            self.initialize_tracker(data_set)
            self.get_download_url_ids(data_set, year)
            spinner.text = self.get_download_progress_report(data_set)
            for url_id in self.download_url_ids[:]:
                filepath = self.scraped_data.get_html(data_set, url_id)
                if not filepath:
                    self.tracker[data_set]["failed"].append(url_id)
                if filepath.exists() and filepath.stat().st_size > ONE_KB:
                    self.tracker[data_set]["retrieved"].append(url_id)
                else:
                    self.tracker[data_set]["failed"].append(url_id)
                self.download_url_ids.remove(url_id)
                spinner.text = self.get_download_progress_report(data_set)
        spinner.stop()
        spinner.clear()

    def initialize_tracker(self, data_set):
        self.tracker[data_set] = {}
        self.tracker[data_set]["skip"] = []
        self.tracker[data_set]["retrieved"] = []
        self.tracker[data_set]["failed"] = []

    def get_download_url_ids(self, data_set, year):
        local_url_ids = self.scraped_data.get_scraped_ids_from_local_folder(
            DocFormat.HTML, data_set, year
        )
        s3_url_ids = self.scraped_data.get_scraped_ids_from_s3(DocFormat.HTML, data_set, year)
        self.download_url_ids = list(set(s3_url_ids) - set(local_url_ids))
        self.tracker[data_set]["skip"] = local_url_ids

    def skip_count(self, data_set):
        return len(self.tracker[data_set]["skip"]) if data_set in self.tracker else 0

    def retrieved_count(self, data_set):
        return len(self.tracker[data_set]["retrieved"]) if data_set in self.tracker else 0

    def failed_count(self, data_set):
        return len(self.tracker[data_set]["failed"]) if data_set in self.tracker else 0

    def total_complete(self, data_set):
        return self.retrieved_count(data_set) + self.failed_count(data_set)

    def total_remaining(self):
        return len(self.download_url_ids)

    def get_download_progress_report(self, data_set):
        total_urls = self.total_complete(data_set) + self.total_remaining()
        percent_complete = 0
        if total_urls:
            percent_complete = self.total_complete(data_set) / float(total_urls)
        return (
            f"Downloading {data_set} HTML from S3... {percent_complete:.0%} "
            f"({self.skip_count(data_set)} Cached, {self.retrieved_count(data_set)} Downloaded, "
            f"{self.failed_count(data_set)} Missing, {self.total_remaining()} Remaining)"
        )
