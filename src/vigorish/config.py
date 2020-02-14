"""Functions that enable reading/writing the config file."""
from __future__ import annotations

import errno
import json
import os
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Mapping, Union, Any, List

from vigorish.enums import (
    DataSet,
    ConfigDataType,
    HtmlStorage,
    JsonStorage,
    ScrapeCondition,
    ScrapeTool,
    StatusReport,
)
from vigorish.util.list_helpers import report_dict
from vigorish.util.result import Result
from vigorish.util.string_helpers import wrap_text


@dataclass
class UrlScrapeDelaySettings:
    delay_is_required: bool
    delay_is_random: bool
    delay_uniform: int
    delay_random_min: int
    delay_random_max: int

    def __str__(self):
        if not self.delay_is_required:
            return "No delay after each URL"
        if not self.delay_is_random:
            return f"Delay is uniform ({self.delay_uniform} seconds)"
        return f"Delay is random ({self.delay_random_min}-{self.delay_random_max} seconds)"

    @property
    def delay_uniform_ms(self):
        return self.delay_uniform * 1000

    @property
    def delay_random_min_ms(self):
        return self.delay_random_min * 1000

    @property
    def delay_random_max_ms(self):
        return self.delay_random_max * 1000

    @staticmethod
    def from_config(config_dict: Mapping[str, Union[None, bool, int]]) -> UrlScrapeDelaySettings:
        delay_is_required = config_dict.get("URL_SCRAPE_DELAY_IS_REQUIRED", True)
        delay_is_random = config_dict.get("URL_SCRAPE_DELAY_IS_RANDOM", True)
        delay_uniform = config_dict.get("URL_SCRAPE_DELAY_IN_SECONDS", 0)
        delay_random_min = config_dict.get("URL_SCRAPE_DELAY_IN_SECONDS_MIN", 3)
        delay_random_max = config_dict.get("URL_SCRAPE_DELAY_IN_SECONDS_MAX", 6)
        delay_settings = UrlScrapeDelaySettings(
            delay_is_required=delay_is_required,
            delay_is_random=delay_is_random,
            delay_uniform=delay_uniform,
            delay_random_min=delay_random_min,
            delay_random_max=delay_random_max,
        )
        return delay_settings


@dataclass
class BatchJobSettings:
    batched_scraping_enabled: bool
    batch_size_is_random: bool
    batch_size_uniform: int
    batch_size_random_min_ms: int
    batch_size_random_max_ms: int

    def __str__(self):
        if not self.batched_scraping_enabled:
            return "Batched scraping is not enabled"
        if not self.batch_size_is_random:
            return f"Batch size is uniform ({self.batch_size_uniform} URLs)"
        return f"Batch size is random ({self.batch_size_random_min_ms}-{self.batch_size_random_max_ms} URLs)"

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
    delay_uniform: int
    delay_random_min: int
    delay_random_max: int

    def __str__(self):
        if not self.delay_is_required:
            return "No delay after each batch"
        if not self.delay_is_random:
            return f"Delay is uniform ({self.delay_uniform} minutes)"
        return f"Delay is random ({self.delay_random_min}-{self.delay_random_max} minutes)"

    @property
    def delay_uniform_ms(self):
        return self.delay_uniform * 60 * 10000

    @property
    def delay_random_min_ms(self):
        return self.delay_random_min * 60 * 10000

    @property
    def delay_random_max_ms(self):
        return self.delay_random_max * 60 * 10000

    @staticmethod
    def from_config(config_dict: Mapping[str, Union[None, bool, int]]) -> BatchScrapeDelaySettings:
        delay_is_required = config_dict.get("BATCH_SCRAPE_DELAY_IS_REQUIRED", True)
        delay_is_random = config_dict.get("BATCH_SCRAPE_DELAY_IS_RANDOM", True)
        delay_uniform = config_dict.get("BATCH_SCRAPE_DELAY_IN_MINUTES", 0)
        delay_random_min = config_dict.get("BATCH_SCRAPE_DELAY_IN_MINUTES_MIN", 5)
        delay_random_max = config_dict.get("BATCH_SCRAPE_DELAY_IN_MINUTES_MAX", 10)
        delay_settings = BatchScrapeDelaySettings(
            delay_is_required=delay_is_required,
            delay_is_random=delay_is_random,
            delay_uniform=delay_uniform,
            delay_random_min=delay_random_min,
            delay_random_max=delay_random_max,
        )
        return delay_settings


class Config:
    setting_name: str
    config_dict: Mapping[str, Union[None, str, bool, Mapping[str, Union[None, int, bool]]]]

    def __init__(
        self,
        setting_name: str,
        config_dict: Mapping[str, Union[None, str, bool, Mapping[str, Union[None, int, bool]]]],
    ):
        self.setting_name = setting_name
        self.config_dict = config_dict

    @property
    def setting_name_title(self) -> str:
        return " ".join(self.setting_name.split("_")).title()

    @property
    def data_type(self) -> ConfigDataType:
        data_type = self.config_dict.get("DATA_TYPE", "")
        if not data_type:
            return ConfigDataType.NONE
        if data_type == "str":
            return ConfigDataType.STRING
        if data_type == "Enum":
            return ConfigDataType.ENUM
        if data_type == "Numeric":
            return ConfigDataType.NUMERIC

    @property
    def description(self) -> str:
        return self.config_dict.get("DESCRIPTION", "")

    @property
    def possible_values(self) -> List[str]:
        return (
            self.__get_possible_values(self.config_dict.get("ENUM_NAME"))
            if self.data_type == ConfigDataType.ENUM
            else []
        )

    @property
    def is_same_for_all_data_sets(self) -> bool:
        return self.config_dict.get("SAME_SETTING_FOR_ALL_DATA_SETS")

    @property
    def current_settings_str(self) -> str:
        if self.is_same_for_all_data_sets:
            settings_dict = {"ALL_DATA_SETS": self.current_setting(DataSet.NONE)}
        else:
            settings_dict = {
                data_set.name: self.current_setting(data_set)
                for data_set in DataSet
                if data_set != DataSet.NONE
            }
        return report_dict(settings_dict, title="", title_prefix="", title_suffix="").strip()

    @property
    def menu_text(self):
        desc_wrapped = wrap_text(self.description, max_len=60)
        current_settings = self.current_settings_str
        return f"{desc_wrapped}\n\n{current_settings}\n"

    def current_setting(
        self, data_set: DataSet
    ) -> Union[None, str, bool, Mapping[str, Union[None, int, bool]]]:
        setting: Union[str, bool, Mapping[str, Union[None, int, bool]]]
        if self.is_same_for_all_data_sets:
            setting = self.config_dict.get("ALL")
        else:
            setting = self.config_dict.get(data_set.name)
        if self.data_type == ConfigDataType.STRING:
            return setting
        if self.data_type == ConfigDataType.ENUM:
            enum_value = self.__get_enum(self.config_dict.get("ENUM_NAME"), setting)
            return enum_value.name
        if self.data_type == ConfigDataType.NUMERIC:
            return self.__get_object(self.config_dict.get("CLASS_NAME"), setting)

    @staticmethod
    def __get_possible_values(enum_name: str) -> List[str]:
        if enum_name == "ScrapeCondition":
            return [member.name for member in ScrapeCondition]
        if enum_name == "HtmlStorage":
            return [member.name for member in HtmlStorage]
        if enum_name == "JsonStorage":
            return [member.name for member in JsonStorage]
        if enum_name == "ScrapeTool":
            return [member.name for member in ScrapeTool]
        if enum_name == "StatusReport":
            return [member.name for member in StatusReport]
        return []

    @staticmethod
    def __get_enum(enum_name: str, value: str) -> Union[None, Enum]:
        if enum_name == "ScrapeCondition":
            return ScrapeCondition[value]
        if enum_name == "HtmlStorage":
            return HtmlStorage[value]
        if enum_name == "JsonStorage":
            return JsonStorage[value]
        if enum_name == "ScrapeTool":
            return ScrapeTool[value]
        if enum_name == "StatusReport":
            return StatusReport[value]
        return ""

    @staticmethod
    def __get_object(
        class_name: str, config_dict: Mapping[str, Union[None, int, bool]]
    ) -> Union[None, UrlScrapeDelaySettings, BatchJobSettings, BatchScrapeDelaySettings]:
        if class_name == "UrlScrapeDelaySettings":
            return UrlScrapeDelaySettings.from_config(config_dict)
        if class_name == "BatchJobSettings":
            return BatchJobSettings.from_config(config_dict)
        if class_name == "BatchScrapeDelaySettings":
            return BatchScrapeDelaySettings.from_config(config_dict)
        return None


class ConfigFile:
    config_file_path: str
    config_json: Mapping[str, Union[None, str, bool, Mapping[str, Union[None, int, bool]]]]

    def __init__(self, config_file_path: str):
        self.config_file_path = config_file_path
        self.__read_config_file()

    @property
    def all_settings(self) -> Dict[str, Config]:
        return [Config(name, config) for name, config in self.config_json.items()]

    def get_current_setting(self, config_name: str, data_set: DataSet) -> Union[None, Config]:
        current_setting_json = self.config_json.get(config_name)
        return Config(config_name, current_setting_json) if current_setting_json else None

    def __read_config_file(self) -> None:
        if not self.config_file_path.exists():
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), filepath)
        if not self.config_file_path.is_file():
            raise TypeError(f"Unable to open config file: {filepath}")
        file_contents = self.config_file_path.read_text()
        self.config_json = json.loads(file_contents)
