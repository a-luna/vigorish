"""Functions that enable reading/writing the config file."""
from __future__ import annotations

import errno
import json
import os
from pathlib import Path
from typing import Mapping, Union, List

from vigorish.config.numeric_settings import (
    UrlScrapeDelaySettings,
    BatchJobSettings,
    BatchScrapeDelaySettings,
)
from vigorish.config.typing import (
    NUMERIC_OPTIONS_JSON_VALUE,
    NUMERIC_JSON_SETTING,
    JSON_CONFIG_VALUE,
    JSON_CONFIG_SETTING,
    NUMERIC_PROMPT_VALUE,
)
from vigorish.enums import (
    DataSet,
    ConfigDataType,
    HtmlStorage,
    JsonStorage,
    ScrapeCondition,
    ScrapeTool,
    StatusReport,
)
from vigorish.util.list_helpers import report_dict, dict_to_param_list
from vigorish.util.result import Result
from vigorish.util.string_helpers import wrap_text


ENUM_PY_SETTING = Union[None, ScrapeCondition, HtmlStorage, JsonStorage, ScrapeTool, StatusReport]
NUMERIC_PY_SETTING = Union[UrlScrapeDelaySettings, BatchJobSettings, BatchScrapeDelaySettings]
PY_SETTING = Union[None, str, ENUM_PY_SETTING, NUMERIC_PY_SETTING]


class Config:
    setting_name: str

    def __init__(self, setting_name: str, config_dict: JSON_CONFIG_VALUE):
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
        return ConfigDataType.NONE

    @property
    def description(self) -> str:
        return self.config_dict.get("DESCRIPTION", "")

    @property
    def possible_values(self) -> List[ENUM_PY_SETTING]:
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
    def menu_text(self) -> str:
        desc_wrapped = wrap_text(self.description, max_len=60)
        current_settings = self.current_settings_str
        return f"{desc_wrapped}\n\n{current_settings}\n"

    def current_setting(self, data_set: DataSet) -> PY_SETTING:
        if self.is_same_for_all_data_sets:
            current_setting = self.config_dict.get(data_set.ALL.name)
        else:
            current_setting = self.config_dict.get(data_set.name)
        if self.data_type == ConfigDataType.STRING:
            return current_setting
        if self.data_type == ConfigDataType.ENUM:
            return self.__get_enum(self.config_dict.get("ENUM_NAME"), current_setting)
        if self.data_type == ConfigDataType.NUMERIC:
            return self.__get_object(self.config_dict.get("CLASS_NAME"), current_setting)
        return None

    def __get_possible_values(self) -> List[ENUM_PY_SETTING]:
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
    def all_settings(self) -> Mapping[str, Config]:
        return {name: Config(name, config) for name, config in self.config_json.items()}

    def get_current_setting(self, config_name: str, data_set: DataSet) -> PY_SETTING:
        setting_json = self.config_json.get(config_name)
        config = Config(config_name, setting_json) if setting_json else None
        return config.current_setting(data_set)

    def change_setting(
        self,
        setting_name: str,
        data_set: DataSet,
        new_value: Union[str, ENUM_PY_SETTING, NUMERIC_PROMPT_VALUE],
    ) -> Result:
        setting = self.config_json.get(setting_name)
        if setting:
            setting["SAME_SETTING_FOR_ALL_DATA_SETS"] = data_set == DataSet.ALL
            if setting["DATA_TYPE"] == "str":
                setting[data_set.name] = new_value
                self.__reset_other_data_sets_enum_str(setting, data_set)
            if setting["DATA_TYPE"] == "Enum":
                setting[data_set.name] = new_value.name
                self.__reset_other_data_sets_enum_str(setting, data_set)
            if setting["DATA_TYPE"] == "Numeric":
                setting[data_set.name] = self.__get_object(setting_name, new_value)
                self.__reset_other_data_sets_numeric(setting, data_set)
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

    def __reset_other_data_sets_enum_str(
        self, setting: STR_ENUM_JSON_SETTING, data_set: DataSet
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
            return UrlScrapeDelaySettings.from_tuple(new_value).to_dict()
        if class_name == "BatchJobSettings":
            return BatchJobSettings.from_tuple(new_value).to_dict()
        if class_name == "BatchScrapeDelaySettings":
            return BatchScrapeDelaySettings.from_tuple(new_value).to_dict()
        return None

    def __get_null_object(self, class_name: str) -> Union[None, Mapping[str, None]]:
        if class_name == "UrlScrapeDelaySettings":
            return UrlScrapeDelaySettings.null_object()
        if class_name == "BatchJobSettings":
            return BatchJobSettings.null_object()
        if class_name == "BatchScrapeDelaySettings":
            return BatchScrapeDelaySettings.null_object()
        return None
