"""Base task that defines the template method for a scrape task."""
import json
import subprocess
from abc import ABC, abstractmethod
from functools import partial
from pathlib import Path
from signal import signal, SIGINT
from sys import exit

from halo import Halo
from lxml import html
from lxml.html import HtmlElement
from Naked.toolshed.shell import execute_js
from tqdm import tqdm

from vigorish.cli.util import prompt_user_yes_no, get_data_set_display_name
from vigorish.config.database import ScrapeJob
from vigorish.constants import EMOJI_DICT, JOB_SPINNER_COLORS
from vigorish.enums import (
    DocFormat,
    DataSet,
    HtmlStorageOption,
    JsonStorageOption,
    ScrapeCondition,
    ScrapeTool,
    PythonScrapeTool,
    JobStatus,
)
from vigorish.scrape.progress_bar import (
    game_date_description,
    url_local_description,
    url_s3_description,
    url_parse_description,
    save_json_description,
    updating_description,
)
from vigorish.scrape.url_builder import UrlBuilder
from vigorish.scrape.util import request_url, render_url
from vigorish.util.datetime_util import get_date_range
from vigorish.util.decorators.retry import RetryLimitExceededError
from vigorish.util.list_helpers import flatten_list2d
from vigorish.util.result import Result

APP_FOLDER = Path(__file__).parent.parent
NODEJS_SCRIPT = APP_FOLDER / "nightmarejs" / "scrape_job.js"


def handle_user_cancellation(db_session, active_job, signal_received, frame):
    active_job.status = JobStatus.PAUSED
    subprocess.run(["clear"])
    prompt = "Would you like to cancel the current job? Selecting NO will pause the job, allowing you to resume the job later. Selecting YES will cancel the job forever."
    result = prompt_user_yes_no(prompt=prompt)
    cancel_job = result.value
    if cancel_job:
        active_job.status = JobStatus.CANCELLED
    db_session.commit()
    exit(0)


class ScrapeTaskABC(ABC):
    data_set: DataSet

    def __init__(self, db_job, db_session, config, scraped_data, driver):
        self.db_job = db_job
        self.db_session = db_session
        self.config = config
        self.scraped_data = scraped_data
        self.driver = driver
        self.url_set = {}
        self.python_scrape_tool = PythonScrapeTool.NONE
        self.start_date = self.db_job.start_date
        self.end_date = self.db_job.end_date
        self.season = self.db_job.season
        self.total_days = self.db_job.total_days
        self.url_builder = UrlBuilder(self.config, self.scraped_data)
        self.scrape_condition = self.config.get_current_setting("SCRAPE_CONDITION", self.data_set)
        self.scrape_tool = self.config.get_current_setting("SCRAPE_TOOL", self.data_set)
        self.html_storage = self.config.get_current_setting("HTML_STORAGE", self.data_set)
        self.json_storage = self.config.get_current_setting("JSON_STORAGE", self.data_set)
        self.html_local_folder = self.config.get_current_setting(
            "HTML_LOCAL_FOLDER_PATH", self.data_set
        )
        self.json_local_folder = self.config.get_current_setting(
            "JSON_LOCAL_FOLDER_PATH", self.data_set
        )
        self.url_delay = self.config.get_current_setting("URL_SCRAPE_DELAY", self.data_set)
        self.batch_settings = self.config.get_current_setting("BATCH_JOB_SETTINGS", self.data_set)
        self.batch_delay = self.config.get_current_setting("BATCH_SCRAPE_DELAY", self.data_set)

    @property
    def get_html_local(self):
        get_html_local_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.scraped_data.html_storage.get_html_brooks_games_for_date_local_file,
            DataSet.BROOKS_PITCH_LOGS: self.scraped_data.html_storage.get_html_brooks_pitch_log_local_file,
            DataSet.BROOKS_PITCHFX: self.scraped_data.html_storage.get_html_brooks_pitchfx_local_file,
            DataSet.BBREF_GAMES_FOR_DATE: self.scraped_data.html_storage.get_html_bbref_games_for_date_local_file,
            DataSet.BBREF_BOXSCORES: self.scraped_data.html_storage.get_html_bbref_boxscore_local_file,
        }
        return get_html_local_dict[self.data_set]

    @property
    def get_html_s3(self):
        get_html_s3_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.scraped_data.html_storage.download_html_brooks_games_for_date,
            DataSet.BROOKS_PITCH_LOGS: self.scraped_data.html_storage.download_html_brooks_pitch_log_page,
            DataSet.BROOKS_PITCHFX: self.scraped_data.html_storage.download_html_brooks_pitchfx_log,
            DataSet.BBREF_GAMES_FOR_DATE: self.scraped_data.html_storage.download_html_bbref_games_for_date,
            DataSet.BBREF_BOXSCORES: self.scraped_data.html_storage.download_html_bbref_boxscore,
        }
        return get_html_s3_dict[self.data_set]

    @property
    def write_html_local(self):
        write_html_local_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.scraped_data.html_storage.write_html_brooks_games_for_date_local_file,
            DataSet.BROOKS_PITCH_LOGS: self.scraped_data.html_storage.write_html_brooks_pitch_log_local_file,
            DataSet.BROOKS_PITCHFX: self.scraped_data.html_storage.write_html_brooks_pitchfx_local_file,
            DataSet.BBREF_GAMES_FOR_DATE: self.scraped_data.html_storage.write_html_bbref_games_for_date_local_file,
            DataSet.BBREF_BOXSCORES: self.scraped_data.html_storage.write_html_bbref_boxscore_local_file,
        }
        return write_html_local_dict[self.data_set]

    @property
    def write_html_s3(self):
        write_html_s3_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.scraped_data.html_storage.upload_html_brooks_games_for_date,
            DataSet.BROOKS_PITCH_LOGS: self.scraped_data.html_storage.upload_html_brooks_pitch_log,
            DataSet.BROOKS_PITCHFX: self.scraped_data.html_storage.upload_html_brooks_pitchfx,
            DataSet.BBREF_GAMES_FOR_DATE: self.scraped_data.html_storage.upload_html_bbref_games_for_date,
            DataSet.BBREF_BOXSCORES: self.scraped_data.html_storage.upload_html_bbref_boxscore,
        }
        return write_html_s3_dict[self.data_set]

    @property
    def write_json_local(self):
        write_json_local_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.scraped_data.json_storage.write_json_brooks_games_for_date_local_file,
            DataSet.BROOKS_PITCH_LOGS: self.scraped_data.json_storage.write_json_brooks_pitch_logs_for_game_local_file,
            DataSet.BROOKS_PITCHFX: self.scraped_data.json_storage.write_json_brooks_pitchfx_log_local_file,
            DataSet.BBREF_GAMES_FOR_DATE: self.scraped_data.json_storage.write_json_bbref_games_for_date_local_file,
            DataSet.BBREF_BOXSCORES: self.scraped_data.json_storage.write_json_bbref_boxscore_local_file,
        }
        return write_json_local_dict[self.data_set]

    @property
    def write_json_s3(self):
        write_json_s3_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.scraped_data.json_storage.write_json_brooks_games_for_date_s3,
            DataSet.BROOKS_PITCH_LOGS: self.scraped_data.json_storage.write_json_brooks_pitch_logs_for_game_s3,
            DataSet.BROOKS_PITCHFX: self.scraped_data.json_storage.write_json_brooks_pitchfx_log_s3,
            DataSet.BBREF_GAMES_FOR_DATE: self.scraped_data.json_storage.write_json_bbref_games_for_date_s3,
            DataSet.BBREF_BOXSCORES: self.scraped_data.json_storage.write_json_bbref_boxscore_s3,
        }
        return write_json_s3_dict[self.data_set]

    @property
    def save_html_local(self):
        return (
            self.html_storage == HtmlStorageOption.LOCAL_FOLDER
            or self.html_storage == HtmlStorageOption.BOTH
        )

    @property
    def save_html_s3(self):
        return (
            self.html_storage == HtmlStorageOption.S3_BUCKET
            or self.html_storage == HtmlStorageOption.BOTH
        )

    @property
    def save_json_local(self):
        return (
            self.json_storage == JsonStorageOption.LOCAL_FOLDER
            or self.json_storage == JsonStorageOption.BOTH
        )

    @property
    def save_json_s3(self):
        return (
            self.json_storage == JsonStorageOption.S3_BUCKET
            or self.json_storage == JsonStorageOption.BOTH
        )

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
        self.db_job.status = JobStatus.PREPARING
        self.db_session.commit()
        result = self.validate_local_folder_paths()
        if result.failure:
            return result
        result = self.construct_url_set()
        if result.failure:
            return result
        html_spinner = Halo(color=JOB_SPINNER_COLORS[self.data_set], spinner="dots3")
        result = self.retrieve_scraped_html(html_spinner)
        if result.failure:
            if "skip" in result.error:
                html_spinner.succeed("HTML Scraped")
                return Result.Fail("skip")
            html_spinner.fail(f"Error! {result.error}")
            return result
        missing_html = result.value
        html_spinner.stop_and_persist(html_spinner.frame(), "")
        self.db_job.status = JobStatus.SCRAPING
        self.db_session.commit()
        result = self.scrape_missing_html(missing_html)
        if result.failure:
            html_spinner.fail(f"Error! {result.error}")
            return result
        html_spinner.succeed("HTML Scraped")
        self.db_job.status = JobStatus.PARSING
        self.db_session.commit()
        result = self.parse_data_from_scraped_html()
        if result.failure:
            return result
        return Result.Ok()

    def validate_local_folder_paths(self):
        html_folder_is_valid = True
        json_folder_is_valid = True
        if self.save_html_local:
            html_folder_is_valid = self.html_local_folder.is_valid(year=self.season.year)
        if self.save_json_local:
            json_folder_is_valid = self.html_local_folder.is_valid(year=self.season.year)
        if html_folder_is_valid and json_folder_is_valid:
            return Result.Ok()

        errors = []
        if not html_folder_is_valid:
            folder_path = self.html_local_folder.resolve(year=self.season.year)
            errors.append(f"Error occurred validating html folder path: {folder_path}")
        if not json_folder_is_valid:
            folder_path = self.html_local_folder.resolve(year=self.season.year)
            errors.append(f"Error occurred validating json folder path: {folder_path}")
        return Result.Fail("\n".join(errors))

    def construct_url_set(self):
        spinner = Halo(color=JOB_SPINNER_COLORS[self.data_set], spinner="dots3")
        spinner.text = "Building URL List..."
        spinner.start()
        result = self.url_builder.create_url_set(self.data_set, self.start_date, self.end_date)
        if result.failure:
            spinner.fail(f"Error! {result.error}")
            return result
        self.url_set = result.value
        spinner.succeed("URL List Built")
        return Result.Ok()

    def retrieve_scraped_html(self, spinner):
        skipped = 0
        retrieved = 0
        scrape_urls = []
        missing = []
        spinner.text = f"Retrieving scraped HTML... (0 Skipped, 0 Found, 0 Missing, {self.total_urls} Remaining)"
        spinner.start()
        for game_date, urls in self.url_set.items():
            result = self.check_prerequisites(game_date)
            if result.failure:
                return result
            result = self.check_current_status(game_date)
            if result.failure:
                if "skip" in result.error:
                    skipped += len(urls)
                    remaining = self.total_urls - skipped
                    perc_complete = float(1) - (remaining / float(self.total_urls))
                    spinner.text = f"Retrieving scraped HTML... {perc_complete:.0%} ({skipped} Skipped, {retrieved} Found, {len(missing)} Missing, {remaining} Remaining)"
                    continue
                return result
            scrape_urls.extend(urls)
            remaining = self.total_urls - skipped
            perc_complete = float(1) - (remaining / float(self.total_urls))
            spinner.text = f"Retrieving scraped HTML... {perc_complete:.0%} ({skipped} Skipped, {retrieved} Found, {len(missing)} Missing, {remaining} Remaining)"
        if not scrape_urls:
            return Result.Fail("skip")
        for url_details in scrape_urls:
            url_id = url_details["identifier"]
            html_filepath = self.get_html(url_id)
            if not html_filepath or self.get_file_size_bytes(html_filepath) < 1024:
                missing.append(url_details)
                remaining = self.total_urls - skipped - retrieved - len(missing)
                perc_complete = float(1) - (remaining / float(self.total_urls))
                spinner.text = f"Retrieving scraped HTML... {perc_complete:.0%} ({skipped} Skipped, {retrieved} Found, {len(missing)} Missing, {remaining} Remaining)"
                continue
            retrieved += 1
            url_details["html_filepath"] = html_filepath
            remaining = self.total_urls - skipped - retrieved - len(missing)
            perc_complete = float(1) - (remaining / float(self.total_urls))
            spinner.text = f"Retrieving scraped HTML... {perc_complete:.0%} ({skipped} Skipped, {retrieved} Found, {len(missing)} Missing, {remaining} Remaining)"
        if missing:
            spinner.text = f"Scraping HTML... ({skipped} Skipped, {retrieved} Found, {len(missing)} Missing, {remaining} Remaining)"
        return Result.Ok(missing)

    def get_file_size_bytes(self, filepath):
        return filepath.stat().st_size

    def get_html(self, url_id):
        result = self.get_html_local(url_id)
        if result.failure:
            result = self.get_html_s3(url_id)
            if result.failure:
                return None
        return result.value

    def scrape_missing_html(self, missing_html):
        if not missing_html:
            return Result.Ok()
        if self.scrape_tool == ScrapeTool.NIGHTMAREJS:
            return self.scrape_html_with_nightmarejs(missing_html)
        if self.scrape_tool == ScrapeTool.REQUESTS_SELENIUM:
            return self.scrape_html_with_requests_selenium(missing_html)
        return Result.Fail(f"SCRAPE_TOOL setting either not set or set incorrectly.")

    def scrape_html_with_requests_selenium(self, missing_html):
        with tqdm(total=len(missing_html), unit="url", leave=False, position=1) as pbar:
            for url_details in missing_html:
                url_id = url_details["identifier"]
                pbar.set_description(url_fetch_description(self.data_set, url_id))
                try:
                    html = self.fetch_url(url_details["url"])
                    pbar.set_description(save_html_description(self.data_set, url_id))
                    result = self.save_scraped_html(html, url_id)
                    if result.failure:
                        return result
                    url_details["html_filepath"] = result.value
                    pbar.update()
                except RetryLimitExceededError as e:
                    return Result.Fail(repr(e))
                except Exception as e:
                    return Result.Fail(f"Error: {repr(e)}")

    def fetch_url(self, url):
        if self.python_scrape_tool == PythonScrapeTool.REQUESTS:
            return request_url(url)
        if self.python_scrape_tool == PythonScrapeTool.SELENIUM:
            return render_url(self.driver, url)
        return Result.Fail(f"SCRAPE_TOOL setting either not set or set incorrectly.")

    def scrape_html_with_nightmarejs(self, url_details):
        urls_json = json.dumps(url_details, indent=2, sort_keys=False)
        url_set_filepath = self.db_job.nodejs_filepath
        url_set_filepath.write_text(urls_json)
        args = self.config.get_nodejs_script_params(self.data_set, url_set_filepath)
        success = execute_js(str(NODEJS_SCRIPT), arguments=args)
        if not success:
            return Result.Fail("nodejs process did not exit successfully")
        url_set_filepath.unlink()
        return Result.Ok()

    def save_scraped_html(self, html, identifier):
        if self.save_html_local:
            result_local = self.write_html_local(identifier, html)
            if result_local.failure:
                return result_local
        if self.save_html_s3:
            result_s3 = self.write_html_s3(identifier, html)
            if result_s3.failure:
                return result_s3
        return result_local if result_local else result_s3 if result_s3 else Result.Fail("")

    def parse_data_from_scraped_html(self):
        spinner = Halo(color=JOB_SPINNER_COLORS[self.data_set], spinner="dots3")
        spinner.text = f"Parsing HTML... 0% (0/{self.total_urls} URLs)"
        spinner.start()
        parsed = 0
        for game_date, urls in self.url_set.items():
            for url_details in urls:
                url = url_details["url"]
                url_id = url_details["identifier"]
                scraped_html = url_details["html_filepath"].read_text()
                page_source = html.fromstring(scraped_html, base_url=url)
                result = self.parse_html(page_source, url_id, url)
                if result.failure:
                    spinner.fail(f"Error! {result.error}")
                    return result
                parsed_data = result.value
                result = self.save_parsed_data(parsed_data)
                if result.failure:
                    spinner.fail(f"Error! {result.error}")
                    return result
                url_details["json_filepath"] = result.value
                result = self.update_status(game_date, parsed_data)
                if result.failure:
                    spinner.fail(f"Error! {result.error}")
                    return result
                parsed += 1
                spinner.text = f"Parsing HTML... {parsed / float(self.total_urls):.0%} ({parsed}/{self.total_urls} URLs)"
        spinner.succeed("HTML Parsed")
        return Result.Ok()

    def save_parsed_data(self, parsed_data):
        if self.save_json_local:
            result_local = self.write_json_local(parsed_data)
            if result_local.failure:
                return result_local
        if self.save_json_s3:
            result_s3 = self.write_json_s3(parsed_data)
            if result_s3.failure:
                return result_s3
        return result_local if result_local else result_s3 if result_s3 else Result.Fail("")

    @abstractmethod
    def check_prerequisites(self, game_date):
        pass

    @abstractmethod
    def check_current_status(self, game_date):
        pass

    @abstractmethod
    def parse_html(self, page_source, url_id, url):
        pass

    @abstractmethod
    def update_status(self, game_date, parsed_data):
        pass
