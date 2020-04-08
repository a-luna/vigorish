"""Base task that defines the template method for a scrape task."""
import subprocess
from abc import ABC, abstractmethod
from functools import partial
from pathlib import Path
from signal import signal, SIGINT
from sys import exit

from halo import Halo
from Naked.toolshed.shell import execute_js

from vigorish.cli.util import prompt_user_yes_no
from vigorish.constants import JOB_SPINNER_COLORS
from vigorish.enums import DataSet, ScrapeCondition, JobStatus
from vigorish.scrape.url_builder import UrlBuilder
from vigorish.scrape.url_tracker import UrlTracker
from vigorish.util.datetime_util import get_date_range
from vigorish.util.result import Result

APP_FOLDER = Path(__file__).parent.parent
NODEJS_SCRIPT = APP_FOLDER / "nightmarejs" / "scrape_job.js"


def user_cancelled(db_session, active_job, spinner, signal_received, frame):
    spinner.stop()
    active_job.status = JobStatus.PAUSED
    subprocess.run(["clear"])
    prompt = (
        "Would you like to cancel the current job? Selecting NO will pause the job, "
        "allowing you to resume the job later. Selecting YES will cancel the job forever."
    )
    result = prompt_user_yes_no(prompt=prompt)
    cancel_job = result.value
    if cancel_job:
        active_job.status = JobStatus.CANCELLED
    db_session.commit()
    exit(0)


class ScrapeTaskABC(ABC):
    data_set: DataSet
    tracker: UrlTracker
    spinner: Halo

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

    @property
    def date_range(self):
        return get_date_range(self.start_date, self.end_date)

    def execute(self):
        if self.scrape_condition == ScrapeCondition.NEVER:
            return Result.Fail("skip")
        result = (
            self.task_preparation()
            .on_success(self.construct_url_set)
            .on_success(self.retrieve_scraped_html)
            .on_success(self.scrape_missing_html)
            .on_success(self.parse_data_from_scraped_html)
        )
        self.spinner.stop()
        if result.failure:
            return result
        return Result.Ok()

    def task_preparation(self):
        self.db_job.status = JobStatus.PREPARING
        self.db_session.commit()
        self.spinner = Halo(color=JOB_SPINNER_COLORS[self.data_set], spinner="dots3")
        signal(SIGINT, partial(user_cancelled, self.db_session, self.db_job, self.spinner))
        return Result.Ok()

    def construct_url_set(self):
        self.spinner.text = "Building URL List..."
        self.spinner.start()
        result = self.url_builder.create_url_set(self.data_set)
        if result.failure:
            return result
        self.tracker = UrlTracker(data_set=self.data_set, all_urls=result.value)
        result = self.determine_scrape_urls()
        if result.failure:
            return result
        return Result.Ok()

    def determine_scrape_urls(self):
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
        self.spinner.text = self.tracker.retrieve_html_report
        if not self.tracker.missing_urls:
            return Result.Fail("skip")
        for url in self.tracker.missing_urls[:]:
            self.scraped_data.get_html(self.data_set, url.identifier)
            if url.file_exists_with_content:
                self.tracker.cached_urls.append(url)
            else:
                self.tracker.scrape_urls.append(url)
            self.tracker.missing_urls.remove(url)
            self.spinner.text = self.tracker.retrieve_html_report
        if self.tracker.scrape_urls:
            self.spinner.text = self.tracker.scrape_html_report
        return Result.Ok()

    def scrape_missing_html(self):
        if not self.tracker.scrape_urls:
            return Result.Ok()
        self.spinner.stop_and_persist(self.spinner.frame(), "")
        self.db_job.status = JobStatus.SCRAPING
        self.db_session.commit()
        url_set_filepath = self.db_job.url_set_filepath
        url_set_filepath.write_text(self.tracker.urls_json)
        args = self.config.get_nodejs_script_params(self.data_set, url_set_filepath)
        success = execute_js(str(NODEJS_SCRIPT), arguments=args)
        if not success:
            return Result.Fail("nodejs process did not exit successfully")
        url_set_filepath.unlink()
        self.spinner.text = self.tracker.save_html_report(saved_count=0)
        self.spinner.start()
        for i, url in enumerate(self.tracker.scrape_urls, start=1):
            if not url.scraped_file_exists_with_content:
                error = f"HTML file does not exist or is empty: {url.scraped_file_path}"
                return Result.Fail(error)
            html = url.scraped_page_content
            result = self.scraped_data.save_html(self.data_set, url.identifier, html)
            if result.failure:
                return result
            self.spinner.text = self.tracker.save_html_report(saved_count=i)
        return Result.Ok()

    def parse_data_from_scraped_html(self):
        self.db_job.status = JobStatus.PARSING
        self.db_session.commit()
        parsed = 0
        self.spinner.text = self.tracker.parse_html_report(self.data_set, parsed)
        for game_date, urls in self.tracker.all_urls.items():
            for url in urls:
                if url.identifier not in self.tracker.parse_url_ids:
                    continue
                result = self.parse_html(url.page_content, url.identifier, url.url)
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
    def parse_html(self, page_content, url_id, url):
        pass

    @abstractmethod
    def update_status(self, game_date, parsed_data):
        pass
