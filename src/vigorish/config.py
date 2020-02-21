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

import snoop


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

    def to_dict(self) -> Dict[str, Union[bool, int]]:
        return {
            "URL_SCRAPE_DELAY_IS_REQUIRED": self.delay_is_required,
            "URL_SCRAPE_DELAY_IS_RANDOM": self.delay_is_random,
            "URL_SCRAPE_DELAY_IN_SECONDS": self.delay_uniform,
            "URL_SCRAPE_DELAY_IN_SECONDS_MIN": self.delay_random_min,
            "URL_SCRAPE_DELAY_IN_SECONDS_MAX": self.delay_random_max,
        }

    @staticmethod
    def from_config(config_dict: Mapping[str, Union[None, bool, int]]) -> UrlScrapeDelaySettings:
        delay_is_required = config_dict.get("URL_SCRAPE_DELAY_IS_REQUIRED", True)
        delay_is_random = config_dict.get("URL_SCRAPE_DELAY_IS_RANDOM", True)
        delay_uniform = config_dict.get("URL_SCRAPE_DELAY_IN_SECONDS", 0)
        delay_random_min = config_dict.get("URL_SCRAPE_DELAY_IN_SECONDS_MIN", 3)
        delay_random_max = config_dict.get("URL_SCRAPE_DELAY_IN_SECONDS_MAX", 6)
        return UrlScrapeDelaySettings(
            delay_is_required=delay_is_required,
            delay_is_random=delay_is_random,
            delay_uniform=delay_uniform,
            delay_random_min=delay_random_min,
            delay_random_max=delay_random_max,
        )

    @staticmethod
    def from_tuple(new_value: Tuple[bool, bool, int, int, int]) -> UrlScrapeDelaySettings:
        is_required, is_random, uniform_value, random_min, random_max = new_value
        return UrlScrapeDelaySettings(
            delay_is_required=is_required,
            delay_is_random=is_random,
            delay_uniform=uniform_value,
            delay_random_min=random_min,
            delay_random_max=random_max,
        )

    @staticmethod
    def null_object() -> Dict[str, None]:
        return {
            "URL_SCRAPE_DELAY_IS_REQUIRED": None,
            "URL_SCRAPE_DELAY_IS_RANDOM": None,
            "URL_SCRAPE_DELAY_IN_SECONDS": None,
            "URL_SCRAPE_DELAY_IN_SECONDS_MIN": None,
            "URL_SCRAPE_DELAY_IN_SECONDS_MAX": None,
        }


@dataclass
class BatchJobSettings:
    batched_scraping_enabled: bool
    batch_size_is_random: bool
    batch_size_uniform: int
    batch_size_random_min: int
    batch_size_random_max: int

    def __str__(self):
        if not self.batched_scraping_enabled:
            return "Batched scraping is not enabled"
        if not self.batch_size_is_random:
            return f"Batch size is uniform ({self.batch_size_uniform} URLs)"
        return f"Batch size is random ({self.batch_size_random_min}-{self.batch_size_random_max} URLs)"

    def to_dict(self) -> Dict[str, Union[bool, int]]:
        return {
            "CREATE_BATCHED_SCRAPE_JOBS": self.batched_scraping_enabled,
            "USE_IRREGULAR_BATCH_SIZES": self.batch_size_is_random,
            "BATCH_SIZE": self.batch_size_uniform,
            "BATCH_SIZE_MIN": self.batch_size_random_min,
            "BATCH_SIZE_MAX": self.batch_size_random_max,
        }

    @staticmethod
    def from_config(config_dict: Mapping[str, Union[None, bool, int]]) -> BatchJobSettings:
        batched_scraping_enabled = config_dict.get("CREATE_BATCHED_SCRAPE_JOBS", True)
        batch_size_is_random = config_dict.get("USE_IRREGULAR_BATCH_SIZES", True)
        batch_size_uniform = config_dict.get("BATCH_SIZE", True)
        batch_size_random_min = config_dict.get("BATCH_SIZE_MIN", True)
        batch_size_random_max = config_dict.get("BATCH_SIZE_MAX", True)
        return BatchJobSettings(
            batched_scraping_enabled=batched_scraping_enabled,
            batch_size_is_random=batch_size_is_random,
            batch_size_uniform=batch_size_uniform,
            batch_size_random_min=batch_size_random_min,
            batch_size_random_max=batch_size_random_max,
        )

    @staticmethod
    def from_tuple(new_value: Tuple[bool, bool, int, int, int]) -> BatchJobSettings:
        is_required, is_random, uniform_value, random_min, random_max = new_value
        return BatchJobSettings(
            batched_scraping_enabled=is_required,
            batch_size_is_random=is_random,
            batch_size_uniform=uniform_value,
            batch_size_random_min=random_min,
            batch_size_random_max=random_max,
        )

    @staticmethod
    def null_object() -> Dict[str, None]:
        return {
            "CREATE_BATCHED_SCRAPE_JOBS": None,
            "USE_IRREGULAR_BATCH_SIZES": None,
            "BATCH_SIZE": None,
            "BATCH_SIZE_MIN": None,
            "BATCH_SIZE_MAX": None,
        }


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

    def to_dict(self) -> Dict[str, Union[bool, int]]:
        return {
            "BATCH_SCRAPE_DELAY_IS_REQUIRED": self.delay_is_required,
            "BATCH_SCRAPE_DELAY_IS_RANDOM": self.delay_is_random,
            "BATCH_SCRAPE_DELAY_IN_MINUTES": self.delay_uniform,
            "BATCH_SCRAPE_DELAY_IN_MINUTES_MIN": self.delay_random_min,
            "BATCH_SCRAPE_DELAY_IN_MINUTES_MAX": self.delay_random_max,
        }

    @staticmethod
    def from_config(config_dict: Mapping[str, Union[None, bool, int]]) -> BatchScrapeDelaySettings:
        delay_is_required = config_dict.get("BATCH_SCRAPE_DELAY_IS_REQUIRED", True)
        delay_is_random = config_dict.get("BATCH_SCRAPE_DELAY_IS_RANDOM", True)
        delay_uniform = config_dict.get("BATCH_SCRAPE_DELAY_IN_MINUTES", 0)
        delay_random_min = config_dict.get("BATCH_SCRAPE_DELAY_IN_MINUTES_MIN", 5)
        delay_random_max = config_dict.get("BATCH_SCRAPE_DELAY_IN_MINUTES_MAX", 10)
        return BatchScrapeDelaySettings(
            delay_is_required=delay_is_required,
            delay_is_random=delay_is_random,
            delay_uniform=delay_uniform,
            delay_random_min=delay_random_min,
            delay_random_max=delay_random_max,
        )

    @staticmethod
    def from_tuple(new_value: Tuple[bool, bool, int, int, int]) -> BatchScrapeDelaySettings:
        is_required, is_random, uniform_value, random_min, random_max = new_value
        return BatchScrapeDelaySettings(
            delay_is_required=is_required,
            delay_is_random=is_random,
            delay_uniform=uniform_value,
            delay_random_min=random_min,
            delay_random_max=random_max,
        )

    @staticmethod
    def null_object() -> Dict[str, None]:
        return {
            "BATCH_SCRAPE_DELAY_IS_REQUIRED": None,
            "BATCH_SCRAPE_DELAY_IS_RANDOM": None,
            "BATCH_SCRAPE_DELAY_IN_MINUTES": None,
            "BATCH_SCRAPE_DELAY_IN_MINUTES_MIN": None,
            "BATCH_SCRAPE_DELAY_IN_MINUTES_MAX": None,
        }


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
        return self.__get_possible_values() if self.data_type == ConfigDataType.ENUM else []

    @property
    def is_same_for_all_data_sets(self) -> bool:
        return self.config_dict.get("SAME_SETTING_FOR_ALL_DATA_SETS")

    @is_same_for_all_data_sets.setter
    def is_same_for_all_data_sets(self, is_same: bool) -> None:
        self.config_dict["SAME_SETTING_FOR_ALL_DATA_SETS"] = is_same

    @property
    def current_settings_str(self) -> str:
        if self.is_same_for_all_data_sets:
            settings_dict = {"ALL_DATA_SETS": self.current_setting(DataSet.ALL)}
        else:
            settings_dict = {
                data_set.name: self.current_setting(data_set)
                for data_set in DataSet
                if data_set != DataSet.ALL
            }
        return report_dict(settings_dict, title="", title_prefix="", title_suffix="").strip()

    @property
    def menu_text(self):
        desc_wrapped = wrap_text(self.description, max_len=60)
        current_settings = self.current_settings_str
        return f"{desc_wrapped}\n\n{current_settings}\n"

    def change_setting(
        self,
        data_set: DataSet,
        new_value: Union[
            str,
            ScrapeCondition,
            ScrapeTool,
            StatusReport,
            HtmlStorage,
            JsonStorage,
            UrlScrapeDelaySettings,
            BatchJobSettings,
            BatchScrapeDelaySettings,
        ],
    ) -> None:
        if self.data_type == ConfigDataType.NUMERIC:
            return self.config_dict
        self.config_dict[data_set] = new_value

    def current_setting(
        self, data_set: DataSet,
    ) -> Union[None, str, bool, Mapping[str, Union[None, int, bool]]]:
        current_setting = self.config_dict.get(data_set.name)
        if self.data_type == ConfigDataType.STRING:
            return current_setting
        if self.data_type == ConfigDataType.ENUM:
            current_setting = self.__get_enum(self.config_dict.get("ENUM_NAME"), current_setting)
            return current_setting.name
        if self.data_type == ConfigDataType.NUMERIC:
            return self.__get_object(self.config_dict.get("CLASS_NAME"), current_setting)

    def __get_possible_values(self) -> List[str]:
        enum_name = self.config_dict.get("ENUM_NAME")
        if enum_name == "ScrapeCondition":
            return [member for member in ScrapeCondition]
        if enum_name == "HtmlStorage":
            return [member for member in HtmlStorage]
        if enum_name == "JsonStorage":
            return [member for member in JsonStorage]
        if enum_name == "ScrapeTool":
            return [member for member in ScrapeTool]
        if enum_name == "StatusReport":
            return [member for member in StatusReport]
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
    def all_settings(self) -> Mapping[str, Config]:
        return {name: Config(name, config) for name, config in self.config_json.items()}

    def get_current_setting(self, config_name: str, data_set: DataSet) -> Union[None, Config]:
        setting_json = self.config_json.get(config_name)
        config = Config(config_name, setting_json) if setting_json else None
        return config.current_setting(data_set)

    @snoop(depth=3)
    def change_setting(
        self,
        setting_name: str,
        data_set: DataSet,
        new_value: Union[
            str,
            ScrapeCondition,
            ScrapeTool,
            StatusReport,
            HtmlStorage,
            JsonStorage,
            UrlScrapeDelaySettings,
            BatchJobSettings,
            BatchScrapeDelaySettings,
        ],
    ):
        setting = self.config_json.get(setting_name)
        if setting:
            setting["SAME_SETTING_FOR_ALL_DATA_SETS"] = data_set == DataSet.ALL
            if setting["DATA_TYPE"] == "Numeric":
                setting[data_set.name] = self.__get_object(setting_name, new_value)
                self.__reset_other_data_sets_numeric(setting, data_set)
            if setting["DATA_TYPE"] == "Enum":
                setting[data_set.name] = new_value.name
                self.__reset_other_data_sets_enum_str(setting, data_set)
            if setting["DATA_TYPE"] == "str":
                setting[data_set.name] = new_value
                self.__reset_other_data_sets_enum_str(setting, data_set)
        return self.__write_config_file()

    def __read_config_file(self) -> None:
        if not self.config_file_path.exists():
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), filepath)
        if not self.config_file_path.is_file():
            raise TypeError(f"Unable to open config file: {filepath}")
        file_contents = self.config_file_path.read_text()
        self.config_json = json.loads(file_contents)

    def __write_config_file(self) -> Result:
        try:
            config_json = json.dumps(self.config_json, indent=2, sort_keys=False)
            self.config_file_path.write_text(config_json)
            return Result.Ok()
        except Exception as e:
            error = f"Error: {repr(e)}"
            return Result.Fail(error)

    def __reset_other_data_sets_enum_str(
        self,
        setting: Mapping[str, Union[None, str, bool, Mapping[str, Union[None, int, bool]]]],
        data_set: DataSet,
    ) -> None:
        if data_set == DataSet.ALL:
            setting[DataSet.BBREF_BOXSCORES.name] = None
            setting[DataSet.BBREF_GAMES_FOR_DATE.name] = None
            setting[DataSet.BROOKS_GAMES_FOR_DATE.name] = None
            setting[DataSet.BROOKS_PITCHFX.name] = None
            setting[DataSet.BROOKS_PITCH_LOGS.name] = None
        else:
            setting[DataSet.ALL.name] = None

    def __reset_other_data_sets_numeric(
        self,
        setting: Mapping[str, Union[None, str, bool, Mapping[str, Union[None, int, bool]]]],
        data_set: DataSet,
    ) -> None:
        class_name = setting.get("CLASS_NAME")
        null_object = self.__get_null_object(class_name)
        if data_set == DataSet.ALL:
            setting[DataSet.BBREF_BOXSCORES.name] = null_object
            setting[DataSet.BBREF_GAMES_FOR_DATE.name] = null_object
            setting[DataSet.BROOKS_GAMES_FOR_DATE.name] = null_object
            setting[DataSet.BROOKS_PITCHFX.name] = null_object
            setting[DataSet.BROOKS_PITCH_LOGS.name] = null_object
        else:
            setting[DataSet.ALL.name] = null_object

    def __get_object(
        self, setting_name: str, new_value: Tuple[bool, bool, int, int, int]
    ) -> Union[None, UrlScrapeDelaySettings, BatchJobSettings, BatchScrapeDelaySettings]:
        setting = self.config_json.get(setting_name)
        class_name = setting.get("CLASS_NAME")
        if class_name == "UrlScrapeDelaySettings":
            return UrlScrapeDelaySettings.from_tuple(new_value).to_dict()
        if class_name == "BatchJobSettings":
            return BatchJobSettings.from_tuple(new_value).to_dict()
        if class_name == "BatchScrapeDelaySettings":
            return BatchScrapeDelaySettings.from_tuple(new_value).to_dict()
        return None

    def __get_null_object(
        self, class_name: str
    ) -> Union[None, UrlScrapeDelaySettings, BatchJobSettings, BatchScrapeDelaySettings]:
        if class_name == "UrlScrapeDelaySettings":
            return UrlScrapeDelaySettings.null_object()
        if class_name == "BatchJobSettings":
            return BatchJobSettings.null_object()
        if class_name == "BatchScrapeDelaySettings":
            return BatchScrapeDelaySettings.null_object()
        return None
