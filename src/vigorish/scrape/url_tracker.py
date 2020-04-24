import json

from vigorish.enums import DataSet
from vigorish.util.list_helpers import flatten_list2d


class UrlTracker:
    def __init__(self, db_job, data_set, all_urls):
        self.db_job = db_job
        self.data_set = data_set
        self.all_urls = all_urls
        self.needed_urls = []
        self.missing_urls = []
        self.cached_urls = []
        self.completed_urls = []
        self.skip_url_count = 0
        self.missing_urls_filepath = self.db_job.url_set_filepath

    @property
    def total_urls(self):
        if not self.all_urls:
            return 0
        return len(flatten_list2d([urls for urls in self.all_urls.values()]))

    @property
    def missing_urls_as_json(self):
        return json.dumps([url.as_dict() for url in self.missing_urls], indent=2, sort_keys=False)

    @property
    def parse_urls(self):
        return self.missing_urls + self.completed_urls + self.cached_urls

    @property
    def parse_url_ids(self):
        if self.data_set == DataSet.BROOKS_PITCH_LOGS:
            return list(set([url.identifier[:12] for url in self.parse_urls]))
        return [url.identifier for url in self.parse_urls]

    @property
    def html_scraping_complete(self):
        return all([url.file_exists_with_content for url in self.parse_urls])

    @property
    def identify_html_report(self):
        percent_complete = 0
        total_remaining = 0
        total_complete = self.skip_url_count + len(self.needed_urls)
        if self.all_urls:
            percent_complete = total_complete / float(self.total_urls)
            total_remaining = self.total_urls - total_complete
        return (
            f"Determining URLs to scrape... {percent_complete:.0%} "
            f"({self.skip_url_count} Skip, {len(self.needed_urls)} Scrape, "
            f"{total_remaining} Remaining)"
        )

    @property
    def retrieve_html_report(self):
        total_complete = self.skip_url_count + len(self.cached_urls) + len(self.missing_urls)
        percent_complete = total_complete / float(self.total_urls)
        return (
            f"Retrieving scraped HTML... {percent_complete:.0%} "
            f"({self.skip_url_count} Skipped, {len(self.cached_urls)} Found, "
            f"{len(self.missing_urls)} Missing, {len(self.needed_urls)} Remaining)"
        )

    @property
    def scrape_html_report(self):
        report = f"Scraping missing HTML... ({self.skip_url_count} Skipped,"
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

    def parse_html_report(self, data_set, parsed_count, game_id=None):
        percent_complete = 0
        if len(self.parse_url_ids) > 0:
            percent_complete = parsed_count / float(len(self.parse_url_ids))
        if game_id:
            data_set_str = "PitchFX Logs"
            unit = "URLs"
            if data_set == DataSet.BROOKS_PITCH_LOGS:
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
        match = [url for url in self.parse_urls if url.identifier == url_id]
        return match[0].html if match else None

    def remove_scraped_html(self):
        for url in self.completed_urls:
            if url.scraped_file_path.exists():
                url.scraped_file_path.unlink()
