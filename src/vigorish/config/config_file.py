import errno
import json
import os
from pathlib import Path

from vigorish.config.types.batch_job_settings import BatchJobSettings
from vigorish.config.types.batch_scrape_delay import BatchScrapeDelaySettings
from vigorish.config.types.url_scrape_delay import UrlScrapeDelaySettings
from vigorish.config.config_setting import (
    EnumConfigSetting,
    NumericConfigSetting,
    PathConfigSetting,
    StringConfigSetting,
)
from vigorish.enums import DataSet, HtmlStorageOption, JsonStorageOption
from vigorish.util.list_helpers import dict_to_param_list
from vigorish.util.result import Result

APP_FOLDER = Path(__file__).parent.parent
DEFAULT_CONFIG = APP_FOLDER / "vig.config.json"


class ConfigFile:
    def __init__(self):
        config_file_path = os.getenv("CONFIG_FILE", "")
        if not config_file_path:
            config_file_path = DEFAULT_CONFIG
        self.config_file_path = Path(config_file_path)
        self.config_json = self.__read_config_file()

    @property
    def all_settings(self):
        return {
            name: self.__config_factory(name, config) for name, config in self.config_json.items()
        }

    def get_current_setting(self, setting_name, data_set):
        config_dict = self.config_json.get(setting_name)
        config = self.__config_factory(setting_name, config_dict) if config_dict else None
        return config.current_setting(data_set)

    def change_setting(self, setting_name, data_set, new_value):
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
                result = self.__get_object(setting_name, new_value)
                if result.failure:
                    return result
                config_dict[data_set.name] = result.value
                self.__reset_other_data_sets_numeric(config_dict, data_set)
        return self.__write_config_file()

    def get_all_url_scrape_params(self, data_set):
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
        return script_params

    def get_nodejs_script_args(self, data_set, url_set_filepath):
        script_params = self.get_all_url_scrape_params(data_set)
        script_params["urlSetFilepath"] = url_set_filepath.resolve()
        return dict_to_param_list(script_params)

    def check_url_delay_settings(self, data_sets):
        url_delay_settings = [
            self.get_current_setting("URL_SCRAPE_DELAY", data_set) for data_set in data_sets
        ]
        if all(url_delay.is_valid for url_delay in url_delay_settings):
            return Result.Ok()
        error = "URL delay cannot be disabled and must be at least 3 seconds"
        return Result.Fail(error)

    def s3_bucket_required(self, data_sets):
        html_storage_settings = [
            self.get_current_setting("HTML_STORAGE", data_set) for data_set in data_sets
        ]
        json_storage_settings = [
            self.get_current_setting("JSON_STORAGE", data_set) for data_set in data_sets
        ]
        html_local_storage = all(
            html_storage == HtmlStorageOption.LOCAL_FOLDER
            for html_storage in html_storage_settings
        )
        json_local_storage = all(
            json_storage == JsonStorageOption.LOCAL_FOLDER
            for json_storage in json_storage_settings
        )
        return not html_local_storage or not json_local_storage

    def __read_config_file(self):
        if not self.config_file_path.exists():
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), str(self.config_file_path)
            )
        if not self.config_file_path.is_file():
            raise TypeError(f"Unable to open config file: {self.config_file_path}")
        return json.loads(self.config_file_path.read_text())

    def __write_config_file(self):
        try:
            config_json = json.dumps(self.config_json, indent=2, sort_keys=False)
            self.config_file_path.write_text(config_json)
            return Result.Ok()
        except Exception as e:
            error = f"Error: {repr(e)}"
            return Result.Fail(error)

    def __config_factory(self, setting_name, config_dict):
        config_type = config_dict.get("CONFIG_TYPE")
        if config_type == "Enum":
            return EnumConfigSetting(setting_name, config_dict)
        if config_type == "Numeric":
            return NumericConfigSetting(setting_name, config_dict)
        if config_type == "Path":
            return PathConfigSetting(setting_name, config_dict)
        return StringConfigSetting(setting_name, config_dict)

    def __reset_other_data_sets_enum_str(self, setting, data_set):
        if data_set == DataSet.ALL:
            setting[DataSet.BBREF_BOXSCORES.name] = None
            setting[DataSet.BBREF_GAMES_FOR_DATE.name] = None
            setting[DataSet.BROOKS_GAMES_FOR_DATE.name] = None
            setting[DataSet.BROOKS_PITCHFX.name] = None
            setting[DataSet.BROOKS_PITCH_LOGS.name] = None
        else:
            setting[DataSet.ALL.name] = None

    def __reset_other_data_sets_numeric(self, setting, data_set):
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

    def __get_object(self, setting_name, new_value):
        setting = self.config_json.get(setting_name)
        class_name = setting.get("CLASS_NAME")
        if class_name == "UrlScrapeDelaySettings":
            result = self.__validate_new_url_delay_setting(new_value)
            if result.failure:
                return result
            return Result.Ok(UrlScrapeDelaySettings(*new_value).to_dict())
        if class_name == "BatchJobSettings":
            return Result.Ok(BatchJobSettings(*new_value).to_dict())
        if class_name == "BatchScrapeDelaySettings":
            return Result.Ok(BatchScrapeDelaySettings(*new_value).to_dict())
        return None

    def __validate_new_url_delay_setting(self, new_value):
        is_enabled, is_random, delay_uniform, delay_min, delay_max = new_value
        if not is_enabled:
            return Result.Fail("URL delay cannot be disabled!")
        if not is_random and delay_uniform < 3 or is_random and delay_min < 3:
            return Result.Fail("URL delay min value must be greater than 2 seconds!")
        return Result.Ok()

    def __get_null_object(self, class_name):
        null_data = (None, None, None, None, None)
        if class_name == "UrlScrapeDelaySettings":
            return UrlScrapeDelaySettings(*null_data).to_dict()
        if class_name == "BatchJobSettings":
            return BatchJobSettings(*null_data).to_dict()
        if class_name == "BatchScrapeDelaySettings":
            return BatchScrapeDelaySettings(*null_data).to_dict()
        return None

    def __get_default_nodejs_script_params(self):
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
        self, url_delay_settings, batch_job_settings, batch_delay_settings
    ):
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
