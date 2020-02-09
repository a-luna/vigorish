"""Functions that enable reading/writing the config file."""
from __future__ import annotations

import errno
import json
import os
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Mapping, Union, Any

from vigorish.util.result import Result
from vigorish.enums import DataSet


@dataclass
class UrlScrapeDelaySettings:
    delay_is_required: bool
    delay_is_random: bool
    delay_uniform_ms: int
    delay_random_min_ms: int
    delay_random_max_ms: int

    @staticmethod
    def from_config(config_dict: Mapping[str, Union[None, bool, int]]) -> UrlScrapeDelaySettings:
        delay_is_required = config_dict.get("URL_SCRAPE_DELAY_IS_REQUIRED", True)
        delay_is_random = config_dict.get("URL_SCRAPE_DELAY_IS_RANDOM", True)
        delay_uniform_ms = config_dict.get("URL_SCRAPE_DELAY_IN_SECONDS", 0)
        delay_random_min_ms = config_dict.get("URL_SCRAPE_DELAY_IN_SECONDS_MIN", 3) * 1000
        delay_random_max_ms = config_dict.get("URL_SCRAPE_DELAY_IN_SECONDS_MAX", 6) * 1000
        delay_settings = UrlScrapeDelaySettings(
            delay_is_required=delay_is_required,
            delay_is_random=delay_is_random,
            delay_uniform_ms=delay_uniform_ms,
            delay_random_min_ms=delay_random_min_ms,
            delay_random_max_ms=delay_random_max_ms,
        )
        return delay_settings


@dataclass
class BatchJobSettings:
    batched_scraping_enabled: bool
    batch_size_is_random: bool
    batch_size_uniform: int
    batch_size_random_min_ms: int
    batch_size_random_max_ms: int

    @staticmethod
    def from_config(config_dict: Mapping[str, Union[None, bool, int]]) -> BatchJobSettings:
        batched_scraping_enabled = config_dict.get("CREATE_BATCHED_SCRAPE_JOBS", True)
        batch_size_is_random = config_dict.get("USE_IRREGULAR_BATCH_SIZES", True)
        batch_size_uniform = config_dict.get("BATCH_SIZE", True)
        batch_size_random_min_ms = config_dict.get("BATCH_SIZE_MIN", True)
        batch_size_random_max_ms = config_dict.get("BATCH_SIZE_MAX", True)
        batch_settings = BatchJobSettings(
            batched_scraping_enabled=batched_scraping_enabled,
            batch_size_is_random=batch_size_is_random,
            batch_size_uniform=batch_size_uniform,
            batch_size_random_min_ms=batch_size_random_min_ms,
            batch_size_random_max_ms=batch_size_random_max_ms,
        )
        return batch_settings


@dataclass
class BatchScrapeDelaySettings:
    delay_is_required: bool
    delay_is_random: bool
    delay_uniform_ms: int
    delay_random_min_ms: int
    delay_random_max_ms: int

    @staticmethod
    def from_config(config_dict: Mapping[str, Union[None, bool, int]]) -> BatchScrapeDelaySettings:
        delay_is_required = config_dict.get("BATCH_SCRAPE_DELAY_IS_REQUIRED", True)
        delay_is_random = config_dict.get("BATCH_SCRAPE_DELAY_IS_RANDOM", True)
        delay_uniform_ms = config_dict.get("BATCH_SCRAPE_DELAY_IN_MINUTES", 0)
        delay_random_min_ms = config_dict.get("BATCH_SCRAPE_DELAY_IN_MINUTES_MIN", 5) * 60 * 1000
        delay_random_max_ms = config_dict.get("BATCH_SCRAPE_DELAY_IN_MINUTES_MAX", 10) * 60 * 1000
        delay_settings = BatchScrapeDelaySettings(
            delay_is_required=delay_is_required,
            delay_is_random=delay_is_random,
            delay_uniform_ms=delay_uniform_ms,
            delay_random_min_ms=delay_random_min_ms,
            delay_random_max_ms=delay_random_max_ms,
        )
        return delay_settings


class ScrapeOption(Enum):
    """Allowed values for SCRAPE_DATA_SET config setting."""

    ONLY_MISSING_DATA = auto()
    ALWAYS = auto()
    NEVER = auto()


class UpdateOption(Enum):
    """Allowed values for UPDATE_DATA_SET config setting."""

    ONLY_MISSING_DATA = auto()
    ALWAYS = auto()
    NEVER = auto()


class HtmlStorage(Enum):
    """Allowed values for HTML_STORAGE config setting."""

    LOCAL_FOLDER = auto()
    S3_BUCKET = auto()
    BOTH = auto()


class JsonStorage(Enum):
    """Allowed values for JSON_STORAGE config setting."""

    LOCAL_FOLDER = auto()
    S3_BUCKET = auto()
    BOTH = auto()


class ScrapeTool(Enum):
    """Config setting for scrape tool: requests/selenium or nightmarejs."""

    REQUESTS_SELENIUM = auto()
    NIGHTMAREJS = auto()


class StatusReport(Enum):
    """The type of status report (if any) to display after data has been scraped."""

    SEASON_SUMMARY = auto()
    DATE_SUMMARY = auto()
    DATE_DETAIL = auto()
    NONE = auto()


class Config:
    config_file_path: str
    config_json: Mapping[str, Any]

    def __init__(self, config_file_path: str):
        self.config_file_path = config_file_path
        self.read_config_file()

    def read_config_file(self) -> None:
        if not self.config_file_path.exists():
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), filepath)
        if not self.config_file_path.is_file():
            raise TypeError(f"Unable to open config file: {filepath}")
        file_contents = self.config_file_path.read_text()
        self.config_json = json.loads(file_contents)

    def get_all_settings(self) -> List[str]:
        return self.config_json

    def get_config_options(self, config_name: str) -> Result:
        config_options = self.config_json.get(config_name)
        if not config_options:
            return Result.Fail(f"Invalid config setting: {config_name}")
        return Result.Ok(config_options)

    def get_current_setting(self, config_name: str, data_set: DataSet) -> Union[None, Result]:
        result = self.get_config_options(config_name)
        if result.failure:
            return result
        config_options = result.value
        if config_options.get("SAME_SETTING_FOR_ALL_DATA_SETS"):
            setting = config_options.get("ALL")
        else:
            setting = config_options.get(data_set.name)
        if not setting:
            error = (
                f"Error! Failed to retrieve {config_name} setting for " f"data set {data_set.name}"
            )
            return Result.Fail(error)

        data_type = config_options.get("DATA_TYPE")
        if data_type == "str":
            return Result.Ok(setting)
        if data_type == "Enum":
            enum_value = self.__get_enum(config_options.get("ENUM_NAME"), setting)
            return Result.Ok(enum_value)
        if data_type == "Object":
            setting_object = self.__get_object(config_options.get("CLASS_NAME"), setting)
            return Result.Ok(setting_object)
        return None

    def get_possible_values(self, enum_name: str) -> List[str]:
        if enum_name == "ScrapeOption":
            return [member.name for member in ScrapeOption]
        if enum_name == "UpdateOption":
            return [member.name for member in UpdateOption]
        if enum_name == "HtmlStorage":
            return [member.name for member in HtmlStorage]
        if enum_name == "JsonStorage":
            return [member.name for member in JsonStorage]
        if enum_name == "ScrapeTool":
            return [member.name for member in ScrapeTool]
        if enum_name == "StatusReport":
            return [member.name for member in StatusReport]
        return []

    def __get_enum(self, enum_name: str, value: str) -> Union[None, Enum]:
        if enum_name == "ScrapeOption":
            return ScrapeOption[value]
        if enum_name == "UpdateOption":
            return UpdateOption[value]
        if enum_name == "HtmlStorage":
            return HtmlStorage[value]
        if enum_name == "JsonStorage":
            return JsonStorage[value]
        if enum_name == "ScrapeTool":
            return ScrapeTool[value]
        if enum_name == "StatusReport":
            return StatusReport[value]
        return ""

    def __get_object(
        self, class_name: str, config_dict: Mapping[str, Union[bool, int]]
    ) -> Union[None, UrlScrapeDelaySettings, BatchJobSettings, BatchScrapeDelaySettings]:
        if class_name == "UrlScrapeDelaySettings":
            return UrlScrapeDelaySettings.from_config(config_dict)
        if class_name == "BatchJobSettings":
            return BatchJobSettings.from_config(config_dict)
        if class_name == "BatchScrapeDelaySettings":
            return BatchScrapeDelaySettings.from_config(config_dict)
        return None
