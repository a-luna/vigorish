"""Base task that defines the template method for a scrape task."""
import subprocess
from abc import ABC, abstractmethod
from functools import partial
from pathlib import Path
from signal import signal, SIGINT
from sys import exit

from getch import pause
from halo import Halo
from Naked.toolshed.shell import execute_js
from Naked.toolshed.shell import execute as execute_shell_command

from vigorish.cli.util import print_message
from vigorish.constants import JOB_SPINNER_COLORS
from vigorish.enums import DataSet, ScrapeCondition
from vigorish.scrape.url_builder import UrlBuilder
from vigorish.scrape.url_tracker import UrlTracker
from vigorish.util.datetime_util import get_date_range
from vigorish.util.result import Result
from vigorish.util.sys_helpers import program_is_installed

APP_FOLDER = Path(__file__).parent.parent
NODEJS_SCRIPT = APP_FOLDER / "nightmarejs" / "scrape_job.js"


def user_cancelled(db_session, active_job, spinner, signal_received, frame):
    spinner.stop()
    subprocess.run(["clear"])
    print_message("Job cancelled by user!", fg="yellow", bold=True)
    pause(message="Press any key to continue...")
    exit(0)


class ScrapeTaskABC(ABC):
    data_set: DataSet
    tracker: UrlTracker

    def __init__(self, db_job, db_session, config, scraped_data):
        self.db_job = db_job
        self.db_session = db_session
        self.config = config
        self.scraped_data = scraped_data
        self.start_date = self.db_job.start_date
        self.end_date = self.db_job.end_date
        self.season = self.db_job.season
        self.total_days = self.db_job.total_days
        self.scraped_html_folderpath = self.db_job.get_scraped_html_folderpath(self.data_set)
        self.url_builder = UrlBuilder(self.db_job, self.config, self.scraped_data)
        self.scrape_condition = self.config.get_current_setting("SCRAPE_CONDITION", self.data_set)
        self.spinner = Halo(color=JOB_SPINNER_COLORS[self.data_set], spinner="dots3")

    @property
    def date_range(self):
        return get_date_range(self.start_date, self.end_date)

    def execute(self):
        result = (
            self.initialize()
            .on_success(self.identify_missing_urls)
            .on_success(self.retrieve_scraped_html)
            .on_success(self.scrape_missing_html)
            .on_success(self.parse_scraped_html)
        )
        self.spinner.stop()
        if result.failure:
            return result
        self.tracker.remove_scraped_html()
        return Result.Ok()

    def initialize(self):
        if self.scrape_condition == ScrapeCondition.NEVER:
            return Result.Fail("skip")
        signal(SIGINT, partial(user_cancelled, self.db_session, self.db_job, self.spinner))
        self.spinner.text = "Building URL List..."
        self.spinner.start()
        return self.url_builder.create_url_set(self.data_set)

    def identify_missing_urls(self, all_urls):
        self.tracker = UrlTracker(self.data_set, all_urls)
        self.spinner.text = self.tracker.identify_html_report
        for game_date, urls in self.tracker.all_urls.items():
            result = self.check_prerequisites(game_date)
            if result.failure:
                return result
            result = self.check_current_status(game_date)
            if result.failure:
                if "skip" not in result.error:
                    return result
                self.tracker.skip_url_count += len(urls)
            else:
                self.tracker.missing_urls.extend(urls)
            self.spinner.text = self.tracker.identify_html_report
        return Result.Ok()

    def retrieve_scraped_html(self):
        self.spinner.text = self.tracker.retrieve_html_report(checked_url_count=0)
        if not self.tracker.missing_urls:
            return Result.Fail("skip")
        for checked_url_count, url in enumerate(self.tracker.missing_urls[:], start=1):
            self.scraped_data.get_html(self.data_set, url.identifier)
            if not url.file_exists_with_content:
                continue
            self.tracker.cached_urls.append(url)
            self.tracker.missing_urls.remove(url)
            self.spinner.text = self.tracker.retrieve_html_report(checked_url_count)
        return Result.Ok()

    def scrape_missing_html(self):
        while not self.tracker.html_scraping_complete:
            self.spinner.text = self.tracker.scrape_html_report
            self.spinner.stop_and_persist(self.spinner.frame(), "")
            result = self.invoke_nodejs_script()
            self.spinner.text = self.tracker.save_html_report(saved_count=0)
            self.spinner.start()
            for i, url in enumerate(self.tracker.missing_urls[:], start=1):
                if not url.scraped_file_exists_with_content:
                    continue
                result = self.scraped_data.save_html(self.data_set, url.identifier, url.html)
                if result.failure:
                    return result
                self.tracker.completed_urls.append(url)
                self.tracker.missing_urls.remove(url)
                self.spinner.text = self.tracker.save_html_report(saved_count=i)
        return Result.Ok()

    def invoke_nodejs_script(self):
        missing_urls_filepath = self.db_job.url_set_filepath
        missing_urls_filepath.write_text(self.tracker.missing_urls_as_json)
        script_args = self.config.get_nodejs_script_args(self.data_set, missing_urls_filepath)
        if program_is_installed("node"):
            success = execute_js(str(NODEJS_SCRIPT), arguments=script_args)
        elif program_is_installed("nodejs"):
            success = execute_shell_command(f"nodejs {NODEJS_SCRIPT} {script_args}")
        else:
            return Result.Fail("Node.js is NOT installed!")
        missing_urls_filepath.unlink()
        return Result.Ok() if success else Result.Fail("nodejs script failed")

    def parse_scraped_html(self):
        parsed = 0
        self.spinner.text = self.tracker.parse_html_report(self.data_set, parsed)
        for game_date, urls in self.tracker.all_urls.items():
            for url in urls:
                if url.identifier not in self.tracker.parse_url_ids:
                    continue
                result = self.parse_html(url.html, url.identifier, url.url)
                if result.failure:
                    return result
                parsed_data = result.value
                result = self.scraped_data.save_json(self.data_set, parsed_data)
                if result.failure:
                    return result
                result = self.update_status(game_date, parsed_data)
                if result.failure:
                    return result
                parsed += 1
                self.spinner.text = self.tracker.parse_html_report(self.data_set, parsed)
                self.db_session.commit()
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
