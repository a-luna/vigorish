"""Base task that defines the template method for a scrape task."""
import json
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
from vigorish.util.datetime_util import get_date_range
from vigorish.util.list_helpers import flatten_list2d
from vigorish.util.result import Result

APP_FOLDER = Path(__file__).parent.parent
NODEJS_SCRIPT = APP_FOLDER / "nightmarejs" / "scrape_job.js"


def handle_user_cancellation(db_session, active_job, signal_received, frame):
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
    spinner: Halo

    def __init__(self, db_job, db_session, config, scraped_data):
        self.db_job = db_job
        self.db_session = db_session
        self.config = config
        self.scraped_data = scraped_data
        self.url_set = {}
        self.start_date = self.db_job.start_date
        self.end_date = self.db_job.end_date
        self.season = self.db_job.season
        self.total_days = self.db_job.total_days
        self.url_builder = UrlBuilder(self.config, self.scraped_data)
        self.scrape_condition = self.config.get_current_setting("SCRAPE_CONDITION", self.data_set)

    @property
    def total_urls(self):
        if not self.url_set:
            return 0
        return len(flatten_list2d([urls for urls in self.url_set.values()]))

    @property
    def date_range(self):
        return get_date_range(self.start_date, self.end_date)

    def execute(self):
        if self.scrape_condition == ScrapeCondition.NEVER:
            return Result.Fail("skip")
        signal(SIGINT, partial(handle_user_cancellation, self.db_session, self.db_job))
        result = self.construct_url_set()
        if result.failure:
            self.spinner.stop()
            return result
        (skip, scrape_urls) = result.value
        result = self.retrieve_scraped_html(skip, scrape_urls)
        if result.failure:
            self.spinner.stop()
            if "skip" in result.error:
                return Result.Fail("skip")
            return result
        missing_html = result.value
        result = self.scrape_missing_html(missing_html)
        if result.failure:
            self.spinner.stop()
            return result
        result = self.parse_data_from_scraped_html()
        self.spinner.stop()
        if result.failure:
            return result
        return Result.Ok()

    def construct_url_set(self):
        self.db_job.status = JobStatus.PREPARING
        self.db_session.commit()
        self.spinner = Halo(color=JOB_SPINNER_COLORS[self.data_set], spinner="dots3")
        self.spinner.text = "Building URL List..."
        self.spinner.start()
        result = self.url_builder.create_url_set(self.data_set, self.start_date, self.end_date)
        if result.failure:
            return result
        self.url_set = result.value
        result = self.determine_scrape_urls()
        if result.failure:
            return result
        (skip, scrape_urls) = result.value
        return Result.Ok((skip, scrape_urls))

    def determine_scrape_urls(self):
        skip = 0
        remaining = 0
        scrape_urls = []
        self.spinner.text = (
            f"Determining URLs to scrape... 0% (0 Skip, 0 Scrape, {self.total_urls} Remaining)"
        )
        for game_date, urls in self.url_set.items():
            result = self.check_prerequisites(game_date)
            if result.failure:
                return result
            result = self.check_current_status(game_date)
            if result.failure:
                if "skip" not in result.error:
                    return result
                skip += len(urls)
            else:
                scrape_urls.extend(urls)
            remaining = self.total_urls - skip - len(scrape_urls)
            percent_complete = float(1) - (remaining / float(self.total_urls))
            self.spinner.text = (
                f"Determining URLs to scrape... {percent_complete:.0%} ({skip} Skip, "
                f"{len(scrape_urls)} Scrape, {remaining} Remaining)"
            )
            return Result.Ok((skip, scrape_urls))

    def retrieve_scraped_html(self, skip, scrape_urls):
        remaining = self.total_urls - skip
        retrieved = 0
        missing = []
        self.spinner.text = (
            f"Retrieving scraped HTML... 0% ({skip} Skipped, 0 Found, 0 Missing, {remaining} "
            "Remaining)"
        )
        if not scrape_urls:
            return Result.Fail("skip")
        for url in scrape_urls:
            self.scraped_data.get_html(self.data_set, url.identifier)
            if url.file_exists_with_content:
                retrieved += 1
            else:
                missing.append(url)
            remaining = self.total_urls - skip - retrieved - len(missing)
            percent_complete = float(1) - (remaining / float(self.total_urls))
            self.spinner.text = (
                f"Retrieving scraped HTML... {percent_complete:.0%} "
                f"({skip} Skipped, {retrieved} Found, {len(missing)} Missing, "
                f"{remaining} Remaining)"
            )
        if missing:
            self.spinner.text = (
                f"Scraping HTML... ({skip} Skipped, "
                f"{retrieved} Found, {len(missing)} Missing, {remaining} Remaining)"
            )
        return Result.Ok(missing)

    def get_file_size_bytes(self, filepath):
        return filepath.stat().st_size

    def scrape_missing_html(self, missing):
        if not missing:
            return Result.Ok()
        self.spinner.stop_and_persist(self.spinner.frame(), "")
        self.db_job.status = JobStatus.SCRAPING
        self.db_session.commit()
        url_set_filepath = self.db_job.nodejs_filepath
        urls_json = json.dumps([url.as_dict() for url in missing], indent=2, sort_keys=False)
        url_set_filepath.write_text(urls_json)
        args = self.config.get_nodejs_script_params(self.data_set, url_set_filepath)
        success = execute_js(str(NODEJS_SCRIPT), arguments=args)
        if not success:
            return Result.Fail("nodejs process did not exit successfully")
        url_set_filepath.unlink()
        self.spinner.text = f"Saving scraped HTML... 0% (0/{len(missing)})"
        self.spinner.start()
        for i, url in enumerate(missing, start=1):
            if not url.file_exists_with_content:
                return Result.Fail(f"HTML file does not exist or is empty: {url.local_file_path}")
            result = self.scraped_data.save_html(self.data_set, url.identifier, url.page_content)
            if result.failure:
                return result
            percent_complete = i / float(len(missing))
            self.spinner.text = (
                f"Saving scraped HTML... {percent_complete:.0%}% ({i}/{len(missing)})"
            )
        return Result.Ok()

    def parse_data_from_scraped_html(self):
        self.db_job.status = JobStatus.PARSING
        self.db_session.commit()
        self.spinner.text = f"Parsing HTML... 0% (0/{self.total_urls} URLs)"
        parsed = 0
        for game_date, urls in self.url_set.items():
            for url in urls:
                html_filepath = self.scraped_data.get_html(self.data_set, url.identifier)
                if not html_filepath:
                    return Result.Fail(f"Failed to locate HTML for url: {url.identifier}")
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
                percent_complete = parsed / float(self.total_urls)
                self.spinner.text = (
                    f"Parsing HTML... {percent_complete:.0%} ({parsed}/{self.total_urls} URLs)"
                )
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
