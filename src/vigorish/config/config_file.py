import errno
import json
import os
from copy import deepcopy
from pathlib import Path

from vigorish.config.config_setting import (
    EnumConfigSetting,
    NumericConfigSetting,
    PathConfigSetting,
    StringConfigSetting,
)
from vigorish.config.types.batch_job_settings import BatchJobSettings
from vigorish.config.types.batch_scrape_delay import BatchScrapeDelay
from vigorish.config.types.url_scrape_delay import UrlScrapeDelay
from vigorish.enums import (
    CombinedDataStorageOption,
    DataSet,
    HtmlStorageOption,
    JsonStorageOption,
    ScrapeCondition,
    ScrapeTaskOption,
    StatusReport,
)
from vigorish.util.list_helpers import dict_to_param_list
from vigorish.util.result import Result

VIG_FOLDER = Path.home() / ".vig"
DEFAULT_CONFIG = VIG_FOLDER / "vig.config.json"

# TODO: Create new config setting: DB_TABLE_BATCH_SIZE
#       Used with add_to_database and restore_database tasks


class ConfigFile:
    config_filepath: str

    def __init__(self, config_file_path=None):
        self.config_filepath = config_file_path
        if not self.config_filepath:
            self.config_filepath = Path(os.getenv("CONFIG_FILE"))
        if not self.config_filepath:
            self.config_filepath = DEFAULT_CONFIG
        if isinstance(self.config_filepath, str):
            self.config_filepath = Path(self.config_filepath)
        if self.config_filepath.exists():
            self.read_config_file()
        else:
            self.create_default_config_file()

    @property
    def all_settings(self):
        return {name: self.config_factory(name, config) for name, config in self.config_json.items()}

    def get_current_setting(self, setting_name, data_set=DataSet.ALL, year=None):
        config_setting = self.all_settings.get(setting_name)
        if not config_setting:
            raise ValueError(f"{setting_name} is not a valid configuration setting")
        return (
            config_setting.current_setting(data_set).resolve(year)
            if isinstance(config_setting, PathConfigSetting)
            else config_setting.current_setting(data_set)
        )

    def change_setting(self, setting_name, data_set, new_value):
        config_dict = self.config_json.get(setting_name)
        if config_dict:
            config_dict["SAME_SETTING_FOR_ALL_DATA_SETS"] = data_set == DataSet.ALL
            if config_dict["CONFIG_TYPE"] == "Numeric":
                result = self.get_object(setting_name, new_value)
                if result.failure:
                    return result
                config_dict[data_set.name] = result.value
                self.reset_other_data_sets_numeric(config_dict, data_set)
            else:
                config_dict[data_set.name] = str(new_value)
                self.reset_other_data_sets_enum_str(config_dict, data_set)
        return self.write_config_file()

    def get_all_url_scrape_params(self, data_set):
        url_delay_settings = self.get_current_setting("URL_SCRAPE_DELAY", data_set)
        batch_job_settings = self.get_current_setting("BATCH_JOB_SETTINGS", data_set)
        batch_delay_settings = self.get_current_setting("BATCH_SCRAPE_DELAY", data_set)
        if url_delay_settings and batch_job_settings and batch_delay_settings:
            return self.get_nodejs_script_params_from_objects(
                url_delay_settings, batch_job_settings, batch_delay_settings
            )
        else:
            return self.get_default_nodejs_script_params()

    def get_nodejs_script_args(self, data_set, url_set_filepath):
        script_params = self.get_all_url_scrape_params(data_set)
        script_params["urlSetFilepath"] = url_set_filepath.resolve()
        return dict_to_param_list(script_params)

    def check_url_delay_settings(self, data_sets):
        url_delay_settings = [self.get_current_setting("URL_SCRAPE_DELAY", data_set) for data_set in data_sets]
        if all(url_delay.is_valid for url_delay in url_delay_settings):
            return Result.Ok()
        error = "URL delay cannot be disabled and must be at least 3 seconds"
        return Result.Fail(error)

    def s3_bucket_required(self, data_sets):
        html_storage_settings = [self.get_current_setting("HTML_STORAGE", data_set) for data_set in data_sets]
        json_storage_settings = [self.get_current_setting("JSON_STORAGE", data_set) for data_set in data_sets]
        html_local_storage = all(
            html_storage in [HtmlStorageOption.NONE, HtmlStorageOption.LOCAL_FOLDER]
            for html_storage in html_storage_settings
        )

        json_local_storage = all(
            json_storage == JsonStorageOption.LOCAL_FOLDER for json_storage in json_storage_settings
        )
        return not html_local_storage or not json_local_storage

    def read_config_file(self):
        if not self.config_filepath.exists():
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(self.config_filepath))
        if not self.config_filepath.is_file():
            raise TypeError(f"Unable to open config file: {self.config_filepath}")
        self.config_json = json.loads(self.config_filepath.read_text())

    def write_config_file(self):
        try:
            config_json = json.dumps(self.config_json, indent=2, sort_keys=False)
            self.config_filepath.write_text(config_json)
            return Result.Ok()
        except Exception as e:
            error = f"Error: {repr(e)}"
            return Result.Fail(error)

    def config_factory(self, setting_name, config_dict):
        config_type = config_dict.get("CONFIG_TYPE")
        if config_type == "Enum":
            return EnumConfigSetting(setting_name, config_dict)
        if config_type == "Numeric":
            return NumericConfigSetting(setting_name, config_dict)
        if config_type == "Path":
            return PathConfigSetting(setting_name, config_dict)
        return StringConfigSetting(setting_name, config_dict)

    def reset_other_data_sets_enum_str(self, setting, data_set):
        if data_set == DataSet.ALL:
            setting[DataSet.BBREF_BOXSCORES.name] = None
            setting[DataSet.BBREF_GAMES_FOR_DATE.name] = None
            setting[DataSet.BROOKS_GAMES_FOR_DATE.name] = None
            setting[DataSet.BROOKS_PITCHFX.name] = None
            setting[DataSet.BROOKS_PITCH_LOGS.name] = None
        else:
            setting[DataSet.ALL.name] = None

    def reset_other_data_sets_numeric(self, setting, data_set):
        class_name = setting.get("CLASS_NAME")
        null_object = self.get_null_object(class_name)
        if data_set == DataSet.ALL:
            setting[DataSet.BBREF_BOXSCORES.name] = null_object
            setting[DataSet.BBREF_GAMES_FOR_DATE.name] = null_object
            setting[DataSet.BROOKS_GAMES_FOR_DATE.name] = null_object
            setting[DataSet.BROOKS_PITCHFX.name] = null_object
            setting[DataSet.BROOKS_PITCH_LOGS.name] = null_object
        else:
            setting[DataSet.ALL.name] = null_object

    def get_object(self, setting_name, new_value):
        setting = self.config_json.get(setting_name)
        class_name = setting.get("CLASS_NAME")
        if class_name == "UrlScrapeDelay":
            result = self.validate_new_url_delay_setting(new_value)
            if result.failure:
                return result
            return Result.Ok(UrlScrapeDelay(*new_value).to_dict())
        if class_name == "BatchJobSettings":
            return Result.Ok(BatchJobSettings(*new_value).to_dict())
        if class_name == "BatchScrapeDelay":
            return Result.Ok(BatchScrapeDelay(*new_value).to_dict())

    def validate_new_url_delay_setting(self, new_value):
        is_enabled, is_random, delay_uniform, delay_min, delay_max = new_value
        if not is_enabled:
            return Result.Fail("URL delay cannot be disabled!")
        if not is_random and delay_uniform < 3 or is_random and delay_min < 3:
            return Result.Fail("URL delay min value must be greater than 2 seconds!")
        return Result.Ok()

    def get_null_object(self, class_name):
        if class_name == "UrlScrapeDelay":
            return UrlScrapeDelay.null_object()
        if class_name == "BatchJobSettings":
            return BatchJobSettings.null_object()
        if class_name == "BatchScrapeDelay":
            return BatchScrapeDelay.null_object()

    def get_default_nodejs_script_params(self):
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

    def get_nodejs_script_params_from_objects(self, url_delay_settings, batch_job_settings, batch_delay_settings):
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

    def create_default_config_file(self):
        self.config_json = deepcopy(self.settings_meta)
        for setting, config_dict in self.config_json.items():
            if config_dict["SAME_SETTING_FOR_ALL_DATA_SETS"]:
                config_dict[DataSet.ALL.name] = self.get_default_value(setting, DataSet.ALL)
                for data_set in DataSet:
                    config_dict[data_set.name] = None
            else:
                config_dict[DataSet.ALL.name] = None
                for data_set in DataSet:
                    config_dict[data_set.name] = self.get_default_value(setting, data_set)
        self.write_config_file()

    def get_default_value(self, setting_name, data_set):
        default_value_dict = {
            "STATUS_REPORT": StatusReport.SEASON_SUMMARY.name,
            "S3_BUCKET": "your-bucket",
            "SCRAPE_CONDITION": ScrapeCondition.ONLY_MISSING_DATA.name,
            "SCRAPE_TASK_OPTION": ScrapeTaskOption.BY_DATE.name,
            "URL_SCRAPE_DELAY": UrlScrapeDelay(True, True, None, 3, 6).to_dict(),
            "BATCH_JOB_SETTINGS": BatchJobSettings(True, True, None, 50, 80).to_dict(),
            "HTML_STORAGE": HtmlStorageOption.NONE.name,
            "HTML_LOCAL_FOLDER_PATH": "html_storage/{year}/{data_set}/",
            "HTML_S3_FOLDER_PATH": "{year}/{data_set}/html/",
            "JSON_STORAGE": JsonStorageOption.LOCAL_FOLDER.name,
            "JSON_LOCAL_FOLDER_PATH": "json_storage/{year}/{data_set}/",
            "JSON_S3_FOLDER_PATH": "{year}/{data_set}/",
            "SCRAPED_DATA_COMBINE_CONDITION": ScrapeCondition.ONLY_MISSING_DATA.name,
            "COMBINED_DATA_STORAGE": CombinedDataStorageOption.LOCAL_FOLDER.name,
            "COMBINED_DATA_LOCAL_FOLDER_PATH": "json_storage/{year}/combined_data",
            "COMBINED_DATA_S3_FOLDER_PATH": "{year}/combined_data",
            "DB_BACKUP_FOLDER_PATH": "backup",
        }
        batch_delay_setting_dict = {
            "BBREF_GAMES_FOR_DATE": BatchScrapeDelay(True, True, None, 5, 10).to_dict(),
            "BBREF_BOXSCORES": BatchScrapeDelay(True, True, None, 5, 10).to_dict(),
            "BROOKS_GAMES_FOR_DATE": BatchScrapeDelay(True, True, None, 30, 45).to_dict(),
            "BROOKS_PITCH_LOGS": BatchScrapeDelay(True, True, None, 30, 45).to_dict(),
            "BROOKS_PITCHFX": BatchScrapeDelay(True, True, None, 30, 45).to_dict(),
        }
        if setting_name in default_value_dict:
            return default_value_dict.get(setting_name)
        if setting_name == "BATCH_SCRAPE_DELAY":
            return batch_delay_setting_dict.get(data_set.name)
        raise ValueError(f"{setting_name} is not a valid setting name")

    @property
    def settings_meta(self):
        return {
            "STATUS_REPORT": {
                "CONFIG_TYPE": "Enum",
                "ENUM_NAME": "StatusReport",
                "DESCRIPTION": (
                    "After a scrape job has successfully completed, you can display a report "
                    "for the MLB season displaying various metrics for all data sets. The "
                    "options below determine the level of detail reported."
                ),
                "SAME_SETTING_FOR_ALL_DATA_SETS": True,
            },
            "S3_BUCKET": {
                "CONFIG_TYPE": "str",
                "DESCRIPTION": ("The name of the S3 bucket to use for storing HTML and/or JSON files"),
                "SAME_SETTING_FOR_ALL_DATA_SETS": True,
            },
            "SCRAPE_CONDITION": {
                "CONFIG_TYPE": "Enum",
                "ENUM_NAME": "ScrapeCondition",
                "DESCRIPTION": (
                    "By default, HTML is scraped and parsed only once (ONLY_MISSING_DATA). "
                    "You can overwrite existing data by selecting ALWAYS, or prevent any "
                    "data from being scraped by selecting NEVER."
                ),
                "SAME_SETTING_FOR_ALL_DATA_SETS": True,
            },
            "SCRAPE_TASK_OPTION": {
                "CONFIG_TYPE": "Enum",
                "ENUM_NAME": "ScrapeTaskOption",
                "DESCRIPTION": (
                    [
                        (
                            "A scrape job is created by choosing a start date, an end date and "
                            "any data sets you wish to scrape for each day in the specified date "
                            "range. When the job is ran, a list of dates is created from the "
                            "start and end dates (both endpoints are included in the list).\n"
                        ),
                        (
                            "If SCRAPE_TASK_OPTION == BY_DATE (default behavior), the job is "
                            "executed by iterating through the list of dates. For each data set "
                            "a list of URLs to scrape for the current date is generated. After "
                            "all data sets specified for this job have been scraped, the job "
                            "moves to the next date in the list and scrapes all data sets for "
                            "that date. After all days in the list have been scraped, the job "
                            "ends.\n"
                        ),
                        (
                            "If SCRAPE_TASK_OPTION == BY_DATA_SET, the job is executed by "
                            "iterating through the list of data sets to scrape. For each data "
                            "set, a list of URLs to scrape for all days in the range specified "
                            "by the start and end dates is generated. After all URLs for the "
                            "data set have been scraped, the job moves on to the next data set "
                            "and scrapes all URLs for the specified date range. After all data "
                            "sets have been scraped, the job ends."
                        ),
                    ],
                ),
                "SAME_SETTING_FOR_ALL_DATA_SETS": True,
            },
            "URL_SCRAPE_DELAY": {
                "CONFIG_TYPE": "Numeric",
                "CLASS_NAME": "UrlScrapeDelay",
                "DESCRIPTION": (
                    "As a common courtesy (a.k.a, to avoid being banned), after scraping a "
                    "webpage you should wait for a few seconds before requesting the next "
                    "URL. You can specify a single length of time to use for all URLs, or "
                    "create random delay lengths by specifying a minimum and maximum length "
                    "of time."
                ),
                "SAME_SETTING_FOR_ALL_DATA_SETS": True,
            },
            "BATCH_JOB_SETTINGS": {
                "CONFIG_TYPE": "Numeric",
                "CLASS_NAME": "BatchJobSettings",
                "DESCRIPTION": (
                    "Number of URLs to scrape per batch. You can specify a single amount to "
                    "use for all batches, or create random batch sizes by specifying a "
                    "minimum and maximum batch size."
                ),
                "SAME_SETTING_FOR_ALL_DATA_SETS": True,
            },
            "BATCH_SCRAPE_DELAY": {
                "CONFIG_TYPE": "Numeric",
                "CLASS_NAME": "BatchScrapeDelay",
                "DESCRIPTION": (
                    "Some websites will ban you even if you wait for a few seconds between "
                    "each request. To avoid being banned, you can scrape URLs in batches "
                    "(recommended batch size: ~50 URLs/batch) and wait for a long period of "
                    "time (30-45 minutes) before you begin a new batch. You can specify a "
                    "single length of time to use for all batches or create random delay "
                    "lengths by specifying a minimum and maximum length of time."
                ),
                "SAME_SETTING_FOR_ALL_DATA_SETS": False,
            },
            "HTML_STORAGE": {
                "CONFIG_TYPE": "Enum",
                "ENUM_NAME": "HtmlStorageOption",
                "DESCRIPTION": (
                    "By default, HTML is NOT saved after it has been parsed. However, you "
                    "can choose to save scraped HTML in a local folder, S3 bucket, or "
                    "both."
                ),
                "SAME_SETTING_FOR_ALL_DATA_SETS": True,
            },
            "HTML_LOCAL_FOLDER_PATH": {
                "CONFIG_TYPE": "Path",
                "CLASS_NAME": "LocalFolderPathSetting",
                "DESCRIPTION": (
                    "Local folder path where scraped HTML should be stored. The application "
                    "will always check for saved HTML content in this location before "
                    "sending a request to the website."
                ),
                "SAME_SETTING_FOR_ALL_DATA_SETS": True,
            },
            "HTML_S3_FOLDER_PATH": {
                "CONFIG_TYPE": "Path",
                "CLASS_NAME": "S3FolderPathSetting",
                "DESCRIPTION": (
                    "Path to a folder within an S3 bucket where scraped HTML should be "
                    "stored. The application will always check for saved HTML content in "
                    "this location before sending a request to the website."
                ),
                "SAME_SETTING_FOR_ALL_DATA_SETS": True,
            },
            "JSON_STORAGE": {
                "CONFIG_TYPE": "Enum",
                "ENUM_NAME": "JsonStorageOption",
                "DESCRIPTION": (
                    "MLB data is parsed from HTML and stored in JSON docs. You can store "
                    "the JSON docs in a local folder, S3 bucket, or both."
                ),
                "SAME_SETTING_FOR_ALL_DATA_SETS": True,
            },
            "JSON_LOCAL_FOLDER_PATH": {
                "CONFIG_TYPE": "Path",
                "CLASS_NAME": "LocalFolderPathSetting",
                "DESCRIPTION": "Local folder path where parsed JSON data should be stored.",
                "SAME_SETTING_FOR_ALL_DATA_SETS": True,
            },
            "JSON_S3_FOLDER_PATH": {
                "CONFIG_TYPE": "Path",
                "CLASS_NAME": "S3FolderPathSetting",
                "DESCRIPTION": ("Path to a folder within an S3 bucket where parsed JSON data should be stored."),
                "SAME_SETTING_FOR_ALL_DATA_SETS": True,
            },
            "SCRAPED_DATA_COMBINE_CONDITION": {
                "CONFIG_TYPE": "Enum",
                "ENUM_NAME": "ScrapeCondition",
                "DESCRIPTION": (
                    "By default, the process to combine and audit all scraped data for a "
                    "single game is done only once (ONLY_MISSING_DATA). You can overwrite "
                    "existing data by selecting ALWAYS, or prevent any data from being combined "
                    "by selecting NEVER."
                ),
                "SAME_SETTING_FOR_ALL_DATA_SETS": True,
            },
            "COMBINED_DATA_STORAGE": {
                "CONFIG_TYPE": "Enum",
                "ENUM_NAME": "CombinedDataStorageOption",
                "DESCRIPTION": (
                    "When all data sets have been scraped for a single game, the parsed data is "
                    "audited and combined into a single JSON file. You can choose to store these "
                    "files on your local file system, on S3, or both."
                ),
                "SAME_SETTING_FOR_ALL_DATA_SETS": True,
            },
            "COMBINED_DATA_LOCAL_FOLDER_PATH": {
                "CONFIG_TYPE": "Path",
                "CLASS_NAME": "LocalFolderPathSetting",
                "DESCRIPTION": ("Local folder path where files containing combined game data should be stored."),
                "SAME_SETTING_FOR_ALL_DATA_SETS": True,
            },
            "COMBINED_DATA_S3_FOLDER_PATH": {
                "CONFIG_TYPE": "Path",
                "CLASS_NAME": "S3FolderPathSetting",
                "DESCRIPTION": (
                    "Path to a folder within an S3 bucket where files containing combined game "
                    "data should be stored."
                ),
                "SAME_SETTING_FOR_ALL_DATA_SETS": True,
            },
            "DB_BACKUP_FOLDER_PATH": {
                "CONFIG_TYPE": "Path",
                "CLASS_NAME": "LocalFolderPathSetting",
                "DESCRIPTION": ("Local folder path where csv files containing exported table data should be stored."),
                "SAME_SETTING_FOR_ALL_DATA_SETS": True,
            },
        }
