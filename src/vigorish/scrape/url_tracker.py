import json

from vigorish.enums import DataSet
from vigorish.scrape.url_builder import create_url_set as get_url_set
from vigorish.util.list_helpers import flatten_list2d
from vigorish.util.result import Result


class UrlTracker:
    def __init__(self, db_job, data_set, scraped_data):
        self.db_job = db_job
        self.data_set = data_set
        self.scraped_data = scraped_data
        self.all_urls = []
        self.need_urls = []
        self.missing_urls = []
        self.cached_urls = []
        self.completed_urls = []
        self.skip_urls = []
        self.missing_urls_filepath = self.db_job.url_set_filepath
        self.scraped_html_folderpath = self.db_job.scraped_html_folders[self.data_set]

    @property
    def total_urls(self):
        return (
            len(flatten_list2d([urls_for_date for urls_for_date in self.all_urls.values()]))
            if self.all_urls
            else 0
        )

    @property
    def parse_urls(self):
        return self.missing_urls + self.completed_urls + self.cached_urls

    @property
    def parse_url_ids(self):
        return (
            [url.url_id for url in self.parse_urls]
            if self.data_set != DataSet.BROOKS_PITCH_LOGS
            else list(set([url.url_id[:12] for url in self.parse_urls]))
        )

    @property
    def html_scraping_complete(self):
        return all([url.file_exists_with_content for url in self.parse_urls])

    @property
    def identify_html_report(self):
        percent_complete = 0
        total_remaining = 0
        total_complete = len(self.skip_urls) + len(self.need_urls)
        if self.all_urls:
            percent_complete = total_complete / float(self.total_urls)
            total_remaining = self.total_urls - total_complete
        return (
            f"Determining URLs to scrape... {percent_complete:.0%} "
            f"({len(self.skip_urls)} Skip, {len(self.need_urls)} Scrape, "
            f"{total_remaining} Remaining)"
        )

    @property
    def retrieve_html_report(self):
        total_complete = len(self.skip_urls) + len(self.cached_urls) + len(self.missing_urls)
        percent_complete = total_complete / float(self.total_urls)
        return (
            f"Retrieving scraped HTML... {percent_complete:.0%} "
            f"({len(self.skip_urls)} Skipped, {len(self.cached_urls)} Found, "
            f"{len(self.missing_urls)} Missing, {len(self.need_urls)} Remaining)"
        )

    @property
    def scrape_html_report(self):
        report = f"Scraping missing HTML... ({len(self.skip_urls)} Skipped,"
        if self.completed_urls:
            report = f"{report} {len(self.completed_urls)} Scraped,"
        return f"{report} {len(self.cached_urls)} Found, {len(self.missing_urls)} Missing)"

    @property
    def save_html_report(self):
        total_urls = len(self.missing_urls) + len(self.completed_urls)
        percent_complete = len(self.completed_urls) / float(total_urls)
        return (
            f"Saving scraped HTML... {percent_complete:.0%} "
            f"({len(self.completed_urls)}/{total_urls}) URLs"
        )

    def create_url_set(self):
        result = get_url_set(self.db_job, self.data_set, self.scraped_data)
        if result.failure:
            return result
        self.all_urls = result.value
        return Result.Ok()

    def parse_html_report(self, parsed_count, game_id=None):
        percent_complete = 0
        if len(self.parse_url_ids) > 0:
            percent_complete = parsed_count / float(len(self.parse_url_ids))
        if game_id:
            data_set_str = "PitchFX Logs"
            unit = "URLs"
            if self.data_set == DataSet.BROOKS_PITCH_LOGS:
                data_set_str = "Pitch Logs"
                unit = "Games"
            return (
                f"Parsing {data_set_str} for {game_id}... {percent_complete:.0%} "
                f"({parsed_count}/{len(self.parse_url_ids)}) {unit}"
            )
        return (
            f"Parsing scraped HTML... {percent_complete:.0%} "
            f"({parsed_count}/{len(self.parse_url_ids)}) URLs"
        )

    def create_missing_urls_json_file(self):
        urls_dict = [url.as_dict() for url in self.missing_urls]
        urls_json = json.dumps(urls_dict, indent=2, sort_keys=False)
        self.missing_urls_filepath.write_text(urls_json)
        return self.missing_urls_filepath

    def get_html(self, url_id):
        match = [url for url in self.parse_urls if url.url_id == url_id]
        return match[0].html if match else None

    def remove_scraped_html(self):
        for url in self.completed_urls:
            if url.scraped_file_path.exists():
                url.scraped_file_path.unlink()
