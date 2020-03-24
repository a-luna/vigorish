"""Base task that defines the template method for a scrape task."""
import json
from abc import ABC, abstractmethod
from pathlib import Path

from halo import Halo
from lxml import html
from lxml.html import HtmlElement
from Naked.toolshed.shell import execute_js
from tqdm import tqdm

from vigorish.constants import EMOJI_DICT
from vigorish.enums import (
    DocFormat,
    DataSet,
    HtmlStorageOption,
    JsonStorageOption,
    ScrapeCondition,
    ScrapeTool,
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
from vigorish.util.list_helpers import flatten_list2d
from vigorish.util.result import Result

SRC_FOLDER = Path(__file__).parent.parent.parent
NODEJS_SCRIPT = SRC_FOLDER / "nightmarejs" / "scrape_job.js"


class ScrapeTaskABC(ABC):
    data_set: DataSet

    def __init__(self, db_job, db_session, config, scraped_data, driver, url_builder):
        self.db_job = db_job
        self.db_session = db_session
        self.config = config
        self.scraped_data = scraped_data
        self.driver = driver
        self.url_builder = url_builder
        self.url_set = {}
        self.start_date = self.db_job.start_date
        self.end_date = self.db_job.end_date
        self.season = self.db_job.season
        self.total_days = self.db_job.total_days
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
        return {
            DataSet.BROOKS_GAMES_FOR_DATE: self.scraped_data.html_storage.get_html_brooks_games_for_date_local_file,
            DataSet.BROOKS_PITCH_LOGS: self.scraped_data.html_storage.get_html_brooks_pitch_log_local_file,
            DataSet.BROOKS_PITCHFX: self.scraped_data.html_storage.get_html_brooks_pitchfx_local_file,
            DataSet.BBREF_GAMES_FOR_DATE: self.scraped_data.html_storage.get_html_bbref_games_for_date_local_file,
            DataSet.BBREF_BOXSCORES: self.scraped_data.html_storage.get_html_bbref_boxscore_local_file,
        }

    @property
    def get_html_s3(self):
        return {
            DataSet.BROOKS_GAMES_FOR_DATE: self.scraped_data.html_storage.download_html_brooks_games_for_date,
            DataSet.BROOKS_PITCH_LOGS: self.scraped_data.html_storage.download_html_brooks_pitch_log_page,
            DataSet.BROOKS_PITCHFX: self.scraped_data.html_storage.download_html_brooks_pitchfx_log,
            DataSet.BBREF_GAMES_FOR_DATE: self.scraped_data.html_storage.download_html_bbref_games_for_date,
            DataSet.BBREF_BOXSCORES: self.scraped_data.html_storage.download_html_bbref_boxscore,
        }

    @property
    def write_html_local(self):
        return {
            DataSet.BROOKS_GAMES_FOR_DATE: self.scraped_data.html_storage.write_html_brooks_games_for_date_local_file,
            DataSet.BROOKS_PITCH_LOGS: self.scraped_data.html_storage.write_html_brooks_pitch_log_local_file,
            DataSet.BROOKS_PITCHFX: self.scraped_data.html_storage.write_html_brooks_pitchfx_local_file,
            DataSet.BBREF_GAMES_FOR_DATE: self.scraped_data.html_storage.write_html_bbref_games_for_date_local_file,
            DataSet.BBREF_BOXSCORES: self.scraped_data.html_storage.write_html_bbref_boxscore_local_file,
        }

    @property
    def write_html_s3(self):
        return {
            DataSet.BROOKS_GAMES_FOR_DATE: self.scraped_data.html_storage.upload_html_brooks_games_for_date,
            DataSet.BROOKS_PITCH_LOGS: self.scraped_data.html_storage.upload_html_brooks_pitch_log,
            DataSet.BROOKS_PITCHFX: self.scraped_data.html_storage.upload_html_brooks_pitchfx,
            DataSet.BBREF_GAMES_FOR_DATE: self.scraped_data.html_storage.upload_html_bbref_games_for_date,
            DataSet.BBREF_BOXSCORES: self.scraped_data.html_storage.upload_html_bbref_boxscore,
        }

    @property
    def write_json_local(self):
        return {
            DataSet.BROOKS_GAMES_FOR_DATE: self.scraped_data.json_storage.write_json_brooks_games_for_date_local_file,
            DataSet.BROOKS_PITCH_LOGS: self.scraped_data.json_storage.write_json_brooks_pitch_logs_for_game_local_file,
            DataSet.BROOKS_PITCHFX: self.scraped_data.json_storage.write_json_brooks_pitchfx_log_local_file,
            DataSet.BBREF_GAMES_FOR_DATE: self.scraped_data.json_storage.write_json_bbref_games_for_date_local_file,
            DataSet.BBREF_BOXSCORES: self.scraped_data.json_storage.write_json_bbref_boxscore_local_file,
        }

    @property
    def write_json_s3(self):
        return {
            DataSet.BROOKS_GAMES_FOR_DATE: self.scraped_data.json_storage.write_json_brooks_games_for_date_s3,
            DataSet.BROOKS_PITCH_LOGS: self.scraped_data.json_storage.write_json_brooks_pitch_logs_for_game_s3,
            DataSet.BROOKS_PITCHFX: self.scraped_data.json_storage.write_json_brooks_pitchfx_log_s3,
            DataSet.BBREF_GAMES_FOR_DATE: self.scraped_data.json_storage.write_json_bbref_games_for_date_s3,
            DataSet.BBREF_BOXSCORES: self.scraped_data.json_storage.write_json_bbref_boxscore_s3,
        }

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
        all_urls = flatten_list2d([urls for urls in self.url_set.values()])
        return len(all_urls)

    def execute(self):
        if self.scrape_condition == ScrapeCondition.NEVER:
            return Result.Fail("skip")
        result = self.validate_local_folder_paths()
        if result.failure:
            return Result
        result = self.construct_url_set()
        if result.failure:
            return result
        html_spinner = Halo(color="cyan", spinner="earth")
        result = self.retrieve_scraped_html(html_spinner)
        if result.failure:
            html_spinner.fail(f"Error! {result.error}")
            return result
        missing_html = result.value
        html_spinner.stop_and_persist(html_spinner.frame(), "")
        result = self.scrape_missing_html(missing_html)
        if result.failure:
            html_spinner.fail(f"Error! {result.error}")
            return result
        html_spinner.succeed("HTML Scraped")
        result = self.parse_data_from_scraped_html()
        if result.failure:
            return result

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
        spinner = Halo(text="Building URL List...", color="cyan", spinner="earth")
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
            remaining = self.total_urls - skipped
            perc_complete = float(1) - (remaining / float(self.total_urls))
            spinner.text = f"Retrieving scraped HTML... {perc_complete:.0%} ({skipped} Skipped, {retrieved} Found, {len(missing)} Missing, {remaining} Remaining)"
            result = self.check_prerequisites(game_date)
            if result.failure:
                return result
            result = self.check_current_status(game_date)
            if result.failure:
                if "skip" in result.error:
                    skipped += len(urls)
                    continue
                return result
            scrape_urls.extend(urls)

        for url_details in scrape_urls:
            remaining = self.total_urls - skipped - retrieved - len(missing)
            perc_complete = float(1) - (remaining / float(self.total_urls))
            spinner.text = f"Retrieving scraped HTML... {perc_complete:.0%} ({skipped} Skipped, {retrieved} Found, {len(missing)} Missing, {remaining} Remaining)"
            url_id = url_details["identifier"]
            result = self.get_html_local[self.data_set](url_id)
            if result.failure:
                result = self.get_html_s3[self.data_set](url_id)
                if result.failure:
                    missing.append(url_details)
                    continue
            retrieved += 1
            url_details["html_filepath"] = result.value
        if missing:
            spinner.text = f"Scraping HTML... ({skipped} Skipped, {retrieved} Found, {len(missing)} Missing, {remaining} Remaining)"
        return Result.Ok(missing)

    def scrape_missing_html(self, missing_html):
        if not missing_html:
            return Result.Ok()
        self.db_job.status = JobStatus.SCRAPING
        self.db_session.commit()
        if self.scrape_tool == ScrapeTool.NIGHTMAREJS:
            return self.scrape_html_with_nightmarejs(missing_html)
        if self.scrape_tool == ScrapeTool.REQUESTS_SELENIUM:
            return self.scrape_html_with_requests_selenium(missing_html)
        return Result.Fail(f"SCRAPE_TOOL setting either not set or set incorrectly.")

    def parse_data_from_scraped_html(self):
        spinner = Halo(text="Parsing HTML...", color="cyan", spinner="moon")
        spinner.start()
        self.db_job.status = JobStatus.PARSING
        self.db_session.commit()
        for game_date, urls in self.url_set.items():
            for url_details in urls:
                scraped_html = url_details["html_filepath"].read_text()
                page_source = html.fromstring(scraped_html, base_url=url_details["url"])
                result = self.parse_html(page_source, url_details["url"])
                if result.failure:
                    spinner.fail(f"Error! {result.error}")
                    return result
                parsed_data = result.value
                result = self.save_parsed_data(parsed_data)
                if result.failure:
                    spinner.fail(f"Error! {result.error}")
                    return result
                url_details["json_filepath"] = result.value
                result = self.update_status(parsed_data)
                if result.failure:
                    spinner.fail(f"Error! {result.error}")
                    return result
        spinner.succeed("HTML Parsed")
        return Result.Ok()

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
            result_local = self.write_html_local[self.data_set](identifier, html)
            if result_local.failure:
                return result_local
        if self.save_html_s3:
            result_s3 = self.write_html_s3[self.data_set](identifier, html)
            if result_s3.failure:
                return result_s3
        return result_local if result_local else result_s3 if result_s3 else Result.Fail("")

    def save_parsed_data(self, parsed_data):
        if self.save_json_local:
            result_local = self.write_json_local[self.data_set](parsed_data)
            if result_local.failure:
                return result_local
        if self.save_json_s3:
            result_s3 = self.write_json_s3[self.data_set](parsed_data)
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
    def scrape_html_with_requests_selenium(self, missing_html):
        pass

    @abstractmethod
    def parse_html(self, page_source, url):
        pass

    @abstractmethod
    def update_status(self, parsed_data):
        pass
