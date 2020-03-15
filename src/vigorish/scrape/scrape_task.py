"""Base task that defines the template method for a scrape task."""
from abc import ABC, abstractmethod
from datetime import datetime

from lxml.html import HtmlElement

from vigorish.enums import DataSet
from vigorish.config.database import Season
from vigorish.util.result import Result


class ScrapeTaskABC(ABC):
    data_set: DataSet
    page_content: HtmlElement

    def __init__(self, db, season, config, s3, file_helper, driver):
        self.db = db
        self.season = season
        self.config = config
        self.s3 = s3
        self.file_helper = file_helper
        self.driver = driver
        self.did_scrape_html = False
        self.scrape_condition = self.config.get_current_setting("SCRAPE_CONDITION", self.data_set)
        self.scrape_tool = self.config.get_current_setting("SCRAPE_TOOL", self.data_set)
        self.html_storage = self.config.get_current_setting("HTML_STORAGE", self.data_set)
        self.json_storage = self.config.get_current_setting("JSON_STORAGE", self.data_set)
        self.html_s3_folder = self.config.get_current_setting("HTML_S3_FOLDER_PATH", self.data_set)
        self.json_s3_folder = self.config.get_current_setting("JSON_S3_FOLDER_PATH", self.data_set)
        self.html_folder = self.config.get_current_setting("HTML_LOCAL_FOLDER_PATH", self.data_set)
        self.json_folder = self.config.get_current_setting("JSON_LOCAL_FOLDER_PATH", self.data_set)
        self.url_delay = self.config.get_current_setting("URL_SCRAPE_DELAY", self.data_set)
        self.batch_settings = self.config.get_current_setting("BATCH_JOB_SETTINGS", self.data_set)
        self.batch_delay = self.config.get_current_setting("BATCH_SCRAPE_DELAY", self.data_set)

    def execute(self, scrape_date):
        result = (
            check_prerequisites()
            .on_success(check_current_status)
            .on_success(validate_local_folder_paths)
            .on_success(get_required_input_data)
            .on_success(construct_url_set)
            .on_success(get_html_from_local_folder)
            .on_success(get_html_from_s3)
            .on_success(scrape_html_with_requests_selenium)
            .on_success(scrape_html_with_nightmarejs)
            .on_success(save_html_to_local_folder)
            .on_success(save_html_to_s3)
            .on_success(parse_json_data_from_html)
            .on_success(save_json_to_local_folder)
            .on_success(save_json_to_s3)
            .on_success(update_status)
        )
        if result.failure:
            if "skip" in result.error:
                return Result.Ok()
        return result

    @abstractmethod
    def check_prerequisites(self):
        pass

    @abstractmethod
    def check_current_status(self):
        pass

    @abstractmethod
    def validate_local_folder_paths(self):
        pass

    @abstractmethod
    def get_required_input_data(self):
        pass

    @abstractmethod
    def construct_url_set(self):
        pass

    @abstractmethod
    def get_html_from_local_foldera(self):
        pass

    @abstractmethod
    def get_html_from_s3(self):
        pass

    @abstractmethod
    def scrape_html_with_requests_selenium(self):
        pass

    @abstractmethod
    def scrape_html_with_nightmarejs(self):
        pass

    @abstractmethod
    def save_html_to_local_folder(self):
        pass

    @abstractmethod
    def save_html_to_s3(self):
        pass

    @abstractmethod
    def parse_json_data_from_html(self):
        pass

    @abstractmethod
    def save_json_to_local_folder(self):
        pass

    @abstractmethod
    def save_json_to_s3(self):
        pass

    @abstractmethod
    def update_status(self):
        pass
