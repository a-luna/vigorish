"""Base task that defines the template method for a scrape task."""
import subprocess
from abc import ABC, abstractmethod
from functools import partial
from signal import SIGINT, signal
from sys import exit

from getch import pause
from halo import Halo

from vigorish.cli.components import print_message
from vigorish.config.project_paths import NODEJS_SCRIPT
from vigorish.constants import JOB_SPINNER_COLORS
from vigorish.enums import DataSet, ScrapeCondition
from vigorish.scrape.url_tracker import UrlTracker
from vigorish.util.datetime_util import get_date_range
from vigorish.util.result import Result
from vigorish.util.sys_helpers import execute_nodejs_script


def user_cancelled(db_session, active_job, spinner, signal_received, frame):
    spinner.stop()
    subprocess.run(["clear"])
    print_message("Job cancelled by user!", fg="yellow", bold=True)
    pause(message="Press any key to continue...")
    exit(0)


class ScrapeTaskABC(ABC):
    data_set: DataSet
    url_tracker: UrlTracker

    def __init__(self, db_job, db_session, config, scraped_data):
        self.db_job = db_job
        self.db_session = db_session
        self.config = config
        self.scraped_data = scraped_data
        self.start_date = self.db_job.start_date
        self.end_date = self.db_job.end_date
        self.season = self.db_job.season
        self.total_days = self.db_job.total_days
        self.scrape_condition = self.config.get_current_setting("SCRAPE_CONDITION", self.data_set)
        self.spinner = Halo(color=JOB_SPINNER_COLORS[self.data_set], spinner="dots3")

    @property
    def date_range(self):
        return get_date_range(self.start_date, self.end_date)

    def execute(self):
        result = (
            self.initialize()
            .on_success(self.identify_needed_urls)
            .on_success(self.retrieve_scraped_html)
            .on_success(self.scrape_missing_html)
            .on_success(self.parse_scraped_html)
        )
        self.spinner.stop()
        if result.failure:
            return result
        self.url_tracker.remove_scraped_html()
        return Result.Ok()

    def initialize(self):
        if self.scrape_condition == ScrapeCondition.NEVER:
            return Result.Fail("skip")
        signal(SIGINT, partial(user_cancelled, self.db_session, self.db_job, self.spinner))
        self.spinner.text = "Building URL List..."
        self.spinner.start()
        self.url_tracker = UrlTracker(self.db_job, self.data_set, self.scraped_data)
        result = self.url_tracker.create_url_set()
        return result if self.url_tracker.total_urls else Result.Fail("Unable to generate URL set.")

    def identify_needed_urls(self):
        self.spinner.text = self.url_tracker.identify_html_report
        for game_date, urls in self.url_tracker.all_urls.items():
            result = self.check_prerequisites(game_date)
            if result.failure:
                return result
            result = self.check_current_status(game_date)
            if result.failure:
                if "skip" not in result.error:
                    return result
                self.url_tracker.skip_urls.extend(urls)
            else:
                self.url_tracker.need_urls.extend(urls)
            self.spinner.text = self.url_tracker.identify_html_report
        return Result.Ok()

    def retrieve_scraped_html(self):
        self.spinner.text = self.url_tracker.retrieve_html_report
        if not self.url_tracker.need_urls:
            return Result.Fail("skip")
        for url in self.url_tracker.need_urls[:]:
            self.scraped_data.get_html(self.data_set, url.url_id)
            if not url.file_exists_with_content:
                self.url_tracker.missing_urls.append(url)
            else:
                self.url_tracker.cached_urls.append(url)
            self.url_tracker.need_urls.remove(url)
            self.spinner.text = self.url_tracker.retrieve_html_report
        return Result.Ok()

    def scrape_missing_html(self):
        while not self.url_tracker.html_scraping_complete:
            self.spinner.text = self.url_tracker.scrape_html_report
            self.spinner.stop_and_persist(self.spinner.frame(), "")
            result = self.invoke_nodejs_script()
            self.spinner.text = self.url_tracker.save_html_report
            self.spinner.start()
            for url in self.url_tracker.missing_urls[:]:
                if not url.scraped_file_exists_with_content:
                    continue
                result = self.scraped_data.save_html(self.data_set, url.url_id, url.html)
                if result.failure:
                    return result
                self.url_tracker.completed_urls.append(url)
                self.url_tracker.missing_urls.remove(url)
                self.spinner.text = self.url_tracker.save_html_report
        return Result.Ok()

    def invoke_nodejs_script(self):
        missing_urls_filepath = self.url_tracker.create_missing_urls_json_file()
        script_args = self.config.get_nodejs_script_args(self.data_set, missing_urls_filepath)
        result = execute_nodejs_script(NODEJS_SCRIPT, script_args)
        missing_urls_filepath.unlink()
        return result

    def parse_scraped_html(self):
        parsed = 0
        self.spinner.text = self.url_tracker.parse_html_report(parsed)
        for game_date, urls in self.url_tracker.all_urls.items():
            for url in urls:
                if url.url_id not in self.url_tracker.parse_url_ids:
                    continue
                result = self.parse_html(url.html, url.url_id, url.url)
                if result.failure:
                    if "Unable to parse any game data" in result.error:
                        continue
                    return result
                parsed_data = result.value
                result = self.scraped_data.save_json(self.data_set, parsed_data)
                if result.failure:
                    return result
                result = self.update_status(game_date, parsed_data)
                if result.failure:
                    return result
                self.db_session.commit()
                parsed += 1
                self.spinner.text = self.url_tracker.parse_html_report(parsed)
        return Result.Ok()

    @abstractmethod
    def check_prerequisites(self, game_date):
        pass

    @abstractmethod
    def check_current_status(self, game_date):
        pass

    @abstractmethod
    def parse_html(self, html, url_id, url):
        pass

    @abstractmethod
    def update_status(self, game_date, parsed_data):
        pass
