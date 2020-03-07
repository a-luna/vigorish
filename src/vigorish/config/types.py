"""Functions that enable reading/writing the config file."""
from __future__ import annotations

import errno
import json
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import List, Mapping, Optional, Union, Tuple, TypeVar

from vigorish.enums import (
    ConfigType,
    DataSet,
    HtmlStorage,
    JsonStorage,
    ScrapeCondition,
    ScrapeTool,
    StatusReport,
)
from vigorish.util.list_helpers import report_dict, dict_to_param_list
from vigorish.util.result import Result
from vigorish.util.string_helpers import try_parse_int, wrap_text
from vigorish.util.sys_helpers import validate_folder_path


BASE_JSON_VALUE = Union[None, bool, str]
BASE_JSON_SETTING = Mapping[str, BASE_JSON_VALUE]
NUMERIC_OPTIONS_JSON_VALUE = Mapping[str, Union[None, bool, int]]
NUMERIC_JSON_VALUE = Union[bool, str, NUMERIC_OPTIONS_JSON_VALUE]
NUMERIC_JSON_SETTING = Mapping[str, NUMERIC_JSON_VALUE]
JSON_CONFIG_VALUE = Union[BASE_JSON_SETTING, NUMERIC_JSON_SETTING]
JSON_CONFIG_SETTING = Mapping[str, JSON_CONFIG_VALUE]
NUMERIC_PROMPT_VALUE = Tuple[bool, bool, int, int, int]
ENUM_PY_SETTING = Union[None, ScrapeCondition, HtmlStorage, JsonStorage, ScrapeTool, StatusReport]
TConfigSetting = TypeVar("TConfigSetting", bound="ConfigSetting")


YEAR_TOKEN = "{year}"
DATA_SET_TOKEN = "{data_set}"


class FolderPathSetting:
    _path_str: str

    def __init__(self, path: str, data_set: DataSet):
        self._path_str = path
        self.data_set = data_set

    def __str__(self) -> str:
        return self._path_str

    def is_valid(self, year: Optional[int, str]) -> bool:
        absolute_path = self.resolve(year)
        return validate_folder_path(absolute_path)

    def resolve(self, year: Optional[int, str] = None) -> Path:
        path_str = self._path_str
        if YEAR_TOKEN in self._path_str:
            self._validate_year_value(year)
            if isinstance(year, int):
                year = str(year)
            path_str = path_str.replace(YEAR_TOKEN, year)
        if DATA_SET_TOKEN in self._path_str:
            path_str = path_str.replace(DATA_SET_TOKEN, self.data_set.name)
        return Path(path_str).resolve()

    def _validate_year_value(self, year: Union[int, str]) -> bool:
        if not year:
            error = 'No value provided for "year" parameter, unable to resolve folder path.'
            raise ValueError(error)
        if isinstance(year, str):
            year_str = year
            year = try_parse_int(year_str)
            if not year:
                raise ValueError(f'Failed to parse int value from string "{year_str}"')
        if not isinstance(year, int):
            raise TypeError(f'"year" parameter must be int value (not "{type(year)}").')
        if year < 1900:
            raise ValueError(f"Data is not collected for year={year}")
        if year > date.today().year:
            raise ValueError(f'"{year}" is not valid since it is a future year')


@dataclass
class UrlScrapeDelaySettings:
    delay_is_required: Optional[bool]
    delay_is_random: Optional[bool]
    delay_uniform: Optional[int]
    delay_random_min: Optional[int]
    delay_random_max: Optional[int]

    def __str__(self) -> str:
        if not self.delay_is_required:
            return "No delay after each URL"
        if not self.delay_is_random:
            return f"Delay is uniform ({self.delay_uniform} seconds)"
        return f"Delay is random ({self.delay_random_min}-{self.delay_random_max} seconds)"

    @property
    def delay_uniform_ms(self) -> int:
        return self.delay_uniform * 1000 if self.delay_uniform else 0

    @property
    def delay_random_min_ms(self) -> int:
        return self.delay_random_min * 1000 if self.delay_random_min else 0

    @property
    def delay_random_max_ms(self) -> int:
        return self.delay_random_max * 1000 if self.delay_random_max else 0

    def to_dict(self) -> NUMERIC_OPTIONS_JSON_VALUE:
        return {
            "URL_SCRAPE_DELAY_IS_REQUIRED": self.delay_is_required,
            "URL_SCRAPE_DELAY_IS_RANDOM": self.delay_is_random,
            "URL_SCRAPE_DELAY_IN_SECONDS": self.delay_uniform,
            "URL_SCRAPE_DELAY_IN_SECONDS_MIN": self.delay_random_min,
            "URL_SCRAPE_DELAY_IN_SECONDS_MAX": self.delay_random_max,
        }

    @staticmethod
    def from_config(config_dict: NUMERIC_OPTIONS_JSON_VALUE) -> UrlScrapeDelaySettings:
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
    def null_object() -> Mapping[str, None]:
        return {
            "URL_SCRAPE_DELAY_IS_REQUIRED": None,
            "URL_SCRAPE_DELAY_IS_RANDOM": None,
            "URL_SCRAPE_DELAY_IN_SECONDS": None,
            "URL_SCRAPE_DELAY_IN_SECONDS_MIN": None,
            "URL_SCRAPE_DELAY_IN_SECONDS_MAX": None,
        }


@dataclass
class BatchJobSettings:
    batched_scraping_enabled: Optional[bool]
    batch_size_is_random: Optional[bool]
    batch_size_uniform: Optional[int]
    batch_size_random_min: Optional[int]
    batch_size_random_max: Optional[int]

    def __str__(self) -> str:
        if not self.batched_scraping_enabled:
            return "Batched scraping is not enabled"
        if not self.batch_size_is_random:
            return f"Batch size is uniform ({self.batch_size_uniform} URLs)"
        return f"Batch size is random ({self.batch_size_random_min}-{self.batch_size_random_max} URLs)"

    def to_dict(self) -> NUMERIC_OPTIONS_JSON_VALUE:
        return {
            "CREATE_BATCHED_SCRAPE_JOBS": self.batched_scraping_enabled,
            "USE_IRREGULAR_BATCH_SIZES": self.batch_size_is_random,
            "BATCH_SIZE": self.batch_size_uniform,
            "BATCH_SIZE_MIN": self.batch_size_random_min,
            "BATCH_SIZE_MAX": self.batch_size_random_max,
        }

    @staticmethod
    def from_config(config_dict: NUMERIC_OPTIONS_JSON_VALUE) -> BatchJobSettings:
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
    def null_object() -> Mapping[str, None]:
        return {
            "CREATE_BATCHED_SCRAPE_JOBS": None,
            "USE_IRREGULAR_BATCH_SIZES": None,
            "BATCH_SIZE": None,
            "BATCH_SIZE_MIN": None,
            "BATCH_SIZE_MAX": None,
        }


@dataclass
class BatchScrapeDelaySettings:
    delay_is_required: Optional[bool]
    delay_is_random: Optional[bool]
    delay_uniform: Optional[int]
    delay_random_min: Optional[int]
    delay_random_max: Optional[int]

    def __str__(self) -> str:
        if not self.delay_is_required:
            return "No delay after each batch"
        if not self.delay_is_random:
            return f"Delay is uniform ({self.delay_uniform} minutes)"
        return f"Delay is random ({self.delay_random_min}-{self.delay_random_max} minutes)"

    @property
    def delay_uniform_ms(self) -> int:
        return self.delay_uniform * 60 * 1000 if self.delay_uniform else 0

    @property
    def delay_random_min_ms(self) -> int:
        return self.delay_random_min * 60 * 1000 if self.delay_random_min else 0

    @property
    def delay_random_max_ms(self) -> int:
        return self.delay_random_max * 60 * 1000 if self.delay_random_max else 0

    def to_dict(self) -> NUMERIC_OPTIONS_JSON_VALUE:
        return {
            "BATCH_SCRAPE_DELAY_IS_REQUIRED": self.delay_is_required,
            "BATCH_SCRAPE_DELAY_IS_RANDOM": self.delay_is_random,
            "BATCH_SCRAPE_DELAY_IN_MINUTES": self.delay_uniform,
            "BATCH_SCRAPE_DELAY_IN_MINUTES_MIN": self.delay_random_min,
            "BATCH_SCRAPE_DELAY_IN_MINUTES_MAX": self.delay_random_max,
        }

    @staticmethod
    def from_config(config_dict: NUMERIC_OPTIONS_JSON_VALUE) -> BatchScrapeDelaySettings:
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
    def null_object() -> Mapping[str, None]:
        return {
            "BATCH_SCRAPE_DELAY_IS_REQUIRED": None,
            "BATCH_SCRAPE_DELAY_IS_RANDOM": None,
            "BATCH_SCRAPE_DELAY_IN_MINUTES": None,
            "BATCH_SCRAPE_DELAY_IN_MINUTES_MIN": None,
            "BATCH_SCRAPE_DELAY_IN_MINUTES_MAX": None,
        }


NUMERIC_PY_SETTING = Union[UrlScrapeDelaySettings, BatchJobSettings, BatchScrapeDelaySettings]
PY_SETTING = Union[None, str, ENUM_PY_SETTING, NUMERIC_PY_SETTING, FolderPathSetting]


class ConfigSetting:
    setting_name: str
    config_dict: JSON_CONFIG_VALUE

    def __init__(self, setting_name: str, config_dict: JSON_CONFIG_VALUE):
        self.setting_name = setting_name
        self.config_dict = config_dict

    @property
    def setting_name_title(self) -> str:
        return " ".join(self.setting_name.split("_")).title()

    @property
    def data_type(self) -> ConfigType:
        return ConfigType.NONE

    @property
    def description(self) -> str:
        return self.config_dict.get("DESCRIPTION", "")

    @property
    def possible_values(self) -> None:
        return None

    @property
    def is_same_for_all_data_sets(self) -> bool:
        return self.config_dict.get("SAME_SETTING_FOR_ALL_DATA_SETS")

    @is_same_for_all_data_sets.setter
    def is_same_for_all_data_sets(self, is_same: bool) -> None:
        self.config_dict["SAME_SETTING_FOR_ALL_DATA_SETS"] = is_same

    @property
    def current_settings_report(self) -> str:
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
    def menu_text(self) -> str:
        return (
            f"Setting: {self.setting_name_title} (Type: {self.data_type.name})\n\n"
            f"{wrap_text(self.description, max_len=80)}\n\n"
            f"{wrap_text(self.current_settings_report, max_len=80)}\n"
        )

    def current_setting(self, data_set: DataSet) -> str:
        return (
            self.config_dict.get(data_set.ALL.name)
            if self.is_same_for_all_data_sets
            else self.config_dict.get(data_set.name)
        )


class StringConfigSetting(ConfigSetting):
    @property
    def data_type(self) -> ConfigType:
        return ConfigType.STRING


class PathConfigSetting(ConfigSetting):
    @property
    def data_type(self) -> ConfigType:
        return ConfigType.PATH

    def current_setting(self, data_set: DataSet) -> FolderPathSetting:
        current_setting = super().current_setting(data_set)
        return FolderPathSetting(path=current_setting, data_set=data_set)


class EnumConfigSetting(ConfigSetting):
    @property
    def data_type(self) -> ConfigType:
        return ConfigType.ENUM

    @property
    def possible_values(self) -> List[ENUM_PY_SETTING]:
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

    def current_setting(self, data_set: DataSet) -> ENUM_PY_SETTING:
        current_setting = super().current_setting(data_set)
        return self.__get_enum(self.config_dict.get("ENUM_NAME"), current_setting)

    @staticmethod
    def __get_enum(enum_name: str, value: str) -> ENUM_PY_SETTING:
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
        return None


class NumericConfigSetting(ConfigSetting):
    @property
    def data_type(self) -> ConfigType:
        return ConfigType.NUMERIC

    def current_setting(self, data_set: DataSet) -> NUMERIC_PY_SETTING:
        current_setting = super().current_setting(data_set)
        return self.__get_object(self.config_dict.get("CLASS_NAME"), current_setting)

    @staticmethod
    def __get_object(
        class_name: str, config_dict: NUMERIC_OPTIONS_JSON_VALUE
    ) -> NUMERIC_PY_SETTING:
        if class_name == "UrlScrapeDelaySettings":
            return UrlScrapeDelaySettings.from_config(config_dict)
        if class_name == "BatchJobSettings":
            return BatchJobSettings.from_config(config_dict)
        if class_name == "BatchScrapeDelaySettings":
            return BatchScrapeDelaySettings.from_config(config_dict)
        return None


class ConfigFile:
    config_file_path: Path
    config_json: JSON_CONFIG_SETTING

    def __init__(self, config_file_path: Path):
        self.config_file_path = config_file_path
        self.config_json = self.__read_config_file()

    @property
    def all_settings(self) -> Mapping[str, TConfigSetting]:
        return {
            name: self.__config_factory(name, config) for name, config in self.config_json.items()
        }

    @property
    def all_settings_are_valid(self) -> Result:
        html_storage = self.all_settings.get("HTML_STORAGE")
        html_local_folder = self.all_settings.get("HTML_LOCAL_FOLDER_PATH")
        html_s3_folder = self.all_settings.get("HTML_S3_FOLDER_PATH")
        json_storage = self.all_settings.get("JSON_STORAGE")
        json_local_folder = self.all_settings.get("JSON_LOCAL_FOLDER_PATH")
        json_s3_folder = self.all_settings.get("JSON_S3_FOLDER_PATH")

    def get_current_setting(self, setting_name: str, data_set: DataSet) -> PY_SETTING:
        config_dict = self.config_json.get(setting_name)
        config = self.__config_factory(setting_name, config_dict) if config_dict else None
        return config.current_setting(data_set)

    def change_setting(
        self,
        setting_name: str,
        data_set: DataSet,
        new_value: Union[str, ENUM_PY_SETTING, NUMERIC_PROMPT_VALUE],
    ) -> Result:
        config_dict = self.config_json.get(setting_name)
        if config_dict:
            config_dict["SAME_SETTING_FOR_ALL_DATA_SETS"] = data_set == DataSet.ALL
            if config_dict["CONFIG_TYPE"] == "str":
                config_dict[data_set.name] = new_value
                self.__reset_other_data_sets_enum_str(config_dict, data_set)
            if config_dict["CONFIG_TYPE"] == "Enum":
                config_dict[data_set.name] = new_value.name
                self.__reset_other_data_sets_enum_str(config_dict, data_set)
            if config_dict["CONFIG_TYPE"] == "Numeric":
                config_dict[data_set.name] = self.__get_object(setting_name, new_value)
                self.__reset_other_data_sets_numeric(config_dict, data_set)
        return self.__write_config_file()

    def get_nodejs_script_params(self, data_set: DataSet) -> NUMERIC_OPTIONS_JSON_VALUE:
        url_delay_settings = self.get_current_setting("URL_SCRAPE_DELAY", data_set)
        batch_job_settings = self.get_current_setting("BATCH_JOB_SETTINGS", data_set)
        batch_delay_settings = self.get_current_setting("BATCH_SCRAPE_DELAY", data_set)
        script_params = {}
        if url_delay_settings and batch_job_settings and batch_delay_settings:
            script_params = self.__get_nodejs_script_params_from_objects(
                url_delay_settings, batch_job_settings, batch_delay_settings
            )
        else:
            script_params = self.__get_default_nodejs_script_params()
        return dict_to_param_list(script_params)

    def __read_config_file(self) -> None:
        if not self.config_file_path.exists():
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), str(self.config_file_path)
            )
        if not self.config_file_path.is_file():
            raise TypeError(f"Unable to open config file: {self.config_file_path}")
        return json.loads(self.config_file_path.read_text())

    def __write_config_file(self) -> Result:
        try:
            config_json = json.dumps(self.config_json, indent=2, sort_keys=False)
            self.config_file_path.write_text(config_json)
            return Result.Ok()
        except Exception as e:
            error = f"Error: {repr(e)}"
            return Result.Fail(error)

    def __config_factory(
        self, setting_name: str, config_dict: JSON_CONFIG_VALUE
    ) -> TConfigSetting:
        config_type = config_dict.get("CONFIG_TYPE")
        if config_type == "Enum":
            return EnumConfigSetting(setting_name, config_dict)
        if config_type == "Numeric":
            return NumericConfigSetting(setting_name, config_dict)
        if config_type == "Path":
            return PathConfigSetting(setting_name, config_dict)
        return StringConfigSetting(setting_name, config_dict)

    def __reset_other_data_sets_enum_str(
        self, setting: BASE_JSON_SETTING, data_set: DataSet
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
        self, setting: NUMERIC_JSON_SETTING, data_set: DataSet
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
        self, setting_name: str, new_value: NUMERIC_PROMPT_VALUE
    ) -> Union[None, NUMERIC_OPTIONS_JSON_VALUE]:
        setting = self.config_json.get(setting_name)
        class_name = setting.get("CLASS_NAME")
        if class_name == "UrlScrapeDelaySettings":
            return UrlScrapeDelaySettings(*new_value).to_dict()
        if class_name == "BatchJobSettings":
            return BatchJobSettings(*new_value).to_dict()
        if class_name == "BatchScrapeDelaySettings":
            return BatchScrapeDelaySettings(*new_value).to_dict()
        return None

    def __get_null_object(self, class_name: str) -> Union[None, Mapping[str, None]]:
        null_data = (None, None, None, None, None)
        if class_name == "UrlScrapeDelaySettings":
            return UrlScrapeDelaySettings(*null_data).to_dict()
        if class_name == "BatchJobSettings":
            return BatchJobSettings(*null_data).to_dict()
        if class_name == "BatchScrapeDelaySettings":
            return BatchScrapeDelaySettings(*null_data).to_dict()
        return None

    def __get_default_nodejs_script_params(self) -> NUMERIC_OPTIONS_JSON_VALUE:
        return {
            "urlTimeoutRequired": True,
            "urlTimeoutMinMs": 3000,
            "urlTimeoutMaxMs": 6000,
            "batchTimeoutRequired": True,
            "batchTimeoutMinMs": 1800000,
            "batchTimeoutMaxMs": 2700000,
            "batchScrapingEnabled": True,
            "minBatchSize": 50,
            "maxBatchSize": 80,
        }

    def __get_nodejs_script_params_from_objects(
        self,
        url_delay_settings: UrlScrapeDelaySettings,
        batch_job_settings: BatchJobSettings,
        batch_delay_settings: BatchScrapeDelaySettings,
    ) -> NUMERIC_OPTIONS_JSON_VALUE:
        script_params = {}
        if url_delay_settings.delay_is_required:
            script_params["urlTimeoutRequired"] = True
            if url_delay_settings.delay_is_random:
                script_params["urlTimeoutMinMs"] = url_delay_settings.delay_random_min_ms
                script_params["urlTimeoutMaxMs"] = url_delay_settings.delay_random_max_ms
            else:
                script_params["urlTimeoutUniformMs"] = url_delay_settings.delay_uniform_ms
        if batch_delay_settings.delay_is_required:
            script_params["batchTimeoutRequired"] = True
            if batch_delay_settings.delay_is_random:
                script_params["batchTimeoutMinMs"] = batch_delay_settings.delay_random_min_ms
                script_params["batchTimeoutMaxMs"] = batch_delay_settings.delay_random_max_ms
            else:
                script_params["batchTimeoutUniformMs"] = batch_delay_settings.delay_uniform_ms
        if batch_job_settings.batched_scraping_enabled:
            script_params["batchScrapingEnabled"] = True
            if batch_job_settings.batch_size_is_random:
                script_params["minBatchSize"] = batch_job_settings.batch_size_random_min
                script_params["maxBatchSize"] = batch_job_settings.batch_size_random_max
            else:
                script_params["uniformBatchSize"] = batch_job_settings.batch_size_uniform
        return script_params
