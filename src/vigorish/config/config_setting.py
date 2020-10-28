"""Functions that enable reading/writing the config file."""
from vigorish.config.types.batch_job_settings import BatchJobSettings
from vigorish.config.types.batch_scrape_delay import BatchScrapeDelay
from vigorish.config.types.folder_path_setting import LocalFolderPathSetting, S3FolderPathSetting
from vigorish.config.types.url_scrape_delay import UrlScrapeDelay
from vigorish.enums import (
    CombinedDataStorageOption,
    ConfigType,
    DataSet,
    HtmlStorageOption,
    JsonStorageOption,
    ScrapeCondition,
    StatusReport,
)
from vigorish.util.list_helpers import report_dict


class ConfigSetting:
    setting_name: str

    def __init__(self, setting_name, config_dict):
        self.setting_name = setting_name
        self.config_dict = config_dict

    @property
    def setting_name_title(self):
        return " ".join(self.setting_name.split("_")).title()

    @property
    def data_type(self):
        return ConfigType.NONE

    @property
    def description(self):
        return self.config_dict.get("DESCRIPTION", "")

    @property
    def possible_values(self):
        return None

    @property
    def is_same_for_all_data_sets(self):
        return (
            True
            if self.same_value_for_all_data_sets_is_required
            else self.config_dict.get("SAME_SETTING_FOR_ALL_DATA_SETS")
        )

    @is_same_for_all_data_sets.setter
    def is_same_for_all_data_sets(self, is_same):
        if not self.same_value_for_all_data_sets_is_required:
            self.config_dict["SAME_SETTING_FOR_ALL_DATA_SETS"] = is_same

    @property
    def cannot_be_disabled(self):
        return self.setting_name in ["URL_SCRAPE_DELAY"]

    @property
    def same_value_for_all_data_sets_is_required(self):
        return self.setting_name in [
            "STATUS_REPORT",
            "S3_BUCKET",
            "SCRAPED_DATA_COMBINE_CONDITION",
            "COMBINED_DATA_STORAGE",
            "COMBINED_DATA_LOCAL_FOLDER_PATH",
            "COMBINED_DATA_S3_FOLDER_PATH",
            "DB_BACKUP_FOLDER_PATH",
        ]

    @property
    def current_settings_report(self):
        if self.is_same_for_all_data_sets:
            settings_dict = {"ALL_DATA_SETS": self.current_setting(DataSet.ALL)}
        else:
            settings_dict = {
                data_set.name: self.current_setting(data_set)
                for data_set in DataSet
                if data_set != DataSet.ALL
            }
        return report_dict(settings_dict, title="", title_prefix="", title_suffix="")

    def current_setting(self, data_set: DataSet):
        return (
            self.config_dict.get(DataSet.ALL.name)
            if self.is_same_for_all_data_sets
            else self.config_dict.get(data_set.name)
        )


class StringConfigSetting(ConfigSetting):
    def __repr__(self):
        return (
            "<StringConfigSetting "
            f"setting={self.setting_name},"
            f"value={self.current_settings_report}>"
        )

    @property
    def data_type(self):
        return ConfigType.STRING


class PathConfigSetting(ConfigSetting):
    def __repr__(self):
        return (
            "<PathConfigSetting "
            f"setting={self.setting_name},"
            f"value={self.current_settings_report}>"
        )

    @property
    def data_type(self):
        return ConfigType.PATH

    def current_setting(self, data_set):
        current_setting = super().current_setting(data_set)
        return self.get_object(self.config_dict.get("CLASS_NAME"), current_setting, data_set)

    @staticmethod
    def get_object(class_name, current_setting, data_set):
        if class_name == "LocalFolderPathSetting":
            return LocalFolderPathSetting(path=current_setting, data_set=data_set)
        if class_name == "S3FolderPathSetting":
            return S3FolderPathSetting(path=current_setting, data_set=data_set)
        return None


class EnumConfigSetting(ConfigSetting):
    def __repr__(self):
        return (
            "<EnumConfigSetting "
            f"setting={self.setting_name},"
            f"value={self.current_settings_report}>"
        )

    @property
    def data_type(self):
        return ConfigType.ENUM

    @property
    def possible_values(self):
        enum_name = self.config_dict.get("ENUM_NAME")
        if enum_name == "ScrapeCondition":
            return [member for member in ScrapeCondition]
        if enum_name == "HtmlStorageOption":
            return [member for member in HtmlStorageOption]
        if enum_name == "JsonStorageOption":
            return [member for member in JsonStorageOption]
        if enum_name == "CombinedDataStorageOption":
            return [member for member in CombinedDataStorageOption]
        if enum_name == "StatusReport":
            return [member for member in StatusReport]
        return []

    def current_setting(self, data_set):
        current_setting = super().current_setting(data_set)
        return self.get_enum(self.config_dict.get("ENUM_NAME"), current_setting)

    @staticmethod
    def get_enum(enum_name, value):
        if enum_name == "ScrapeCondition":
            return ScrapeCondition[value]
        if enum_name == "HtmlStorageOption":
            return HtmlStorageOption[value]
        if enum_name == "JsonStorageOption":
            return JsonStorageOption[value]
        if enum_name == "CombinedDataStorageOption":
            return CombinedDataStorageOption[value]
        if enum_name == "StatusReport":
            return StatusReport[value]
        return None


class NumericConfigSetting(ConfigSetting):
    def __repr__(self):
        return (
            "<NumericConfigSetting "
            f"setting={self.setting_name},"
            f"value={self.current_settings_report}>"
        )

    @property
    def data_type(self):
        return ConfigType.NUMERIC

    def current_setting(self, data_set):
        current_setting = super().current_setting(data_set)
        return self.get_object(self.config_dict.get("CLASS_NAME"), current_setting)

    @staticmethod
    def get_object(class_name, config_dict):
        if class_name == "UrlScrapeDelay":
            return UrlScrapeDelay.from_config(config_dict)
        if class_name == "BatchJobSettings":
            return BatchJobSettings.from_config(config_dict)
        if class_name == "BatchScrapeDelay":
            return BatchScrapeDelay.from_config(config_dict)
        return None
