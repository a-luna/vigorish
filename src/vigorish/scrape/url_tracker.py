import json

from vigorish.enums import DataSet
from vigorish.util.list_helpers import flatten_list2d


class UrlTracker:
    def __init__(self, data_set, all_urls):
        self.data_set = data_set
        self.all_urls = all_urls
        self.missing_urls = []
        self.cached_urls = []
        self.scrape_urls = []
        self.skip_url_count = 0

    @property
    def total_urls(self):
        if not self.all_urls:
            return 0
        return len(flatten_list2d([urls for urls in self.all_urls.values()]))

    @property
    def total_complete(self):
        return (
            len(self.missing_urls)
            + len(self.scrape_urls)
            + len(self.cached_urls)
            + self.skip_url_count
        )

    @property
    def parse_urls(self):
        return self.scrape_urls + self.cached_urls

    @property
    def remaining_url_count(self):
        return self.total_urls - self.total_complete

    @property
    def urls_json(self):
        return json.dumps([url.as_dict() for url in self.scrape_urls], indent=2, sort_keys=False)

    @property
    def parse_url_ids(self):
        if self.data_set != DataSet.BROOKS_PITCH_LOGS:
            return [url.identifier for url in self.parse_urls]
        return list(set([url.identifier[:12] for url in self.parse_urls]))

    @property
    def identify_html_report(self):
        percent_complete = 0
        if self.total_urls > 0:
            percent_complete = self.total_complete / float(self.total_urls)
        return (
            f"Determining URLs to scrape... {self.percent_complete:.0%} "
            f"({self.skip_url_count} Skip, {len(self.missing_urls)} Scrape, "
            f"{self.remaining_url_count} Remaining)"
        )

    @property
    def retrieve_html_report(self):
        percent_complete = 0
        if self.total_urls > 0:
            percent_complete = self.total_complete / float(self.total_urls)
        return (
            f"Retrieving scraped HTML... {self.percent_complete:.0%} "
            f"({self.skip_url_count} Skipped, {len(self.cached_urls)} Found, "
            f"{len(self.scrape_urls)} Missing, {self.remaining_url_count} Remaining)"
        )

    @property
    def scrape_html_report(self):
        return (
            f"Scraping missing HTML... ({self.skip_url_count} Skipped, "
            f"{len(self.cached_urls)} Found, {len(self.scrape_urls)} Missing, "
            f"{self.remaining_url_count} Remaining)"
        )

    def save_html_report(self, saved_count):
        percent_complete = 0
        if len(self.scrape_urls) > 0:
            percent_complete = saved_count / float(len(self.scrape_urls))
        return (
            f"Saving scraped HTML... {percent_complete:.0%} "
            f"({saved_count}/{len(self.scrape_urls)}) URLs"
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

    def remove_scraped_html(self):
        for url in self.scrape_urls:
            if url.scraped_file_path.exists():
                url.scraped_file_path.unlink()
