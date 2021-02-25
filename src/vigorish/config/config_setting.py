"""Functions that enable reading/writing the config file."""
from abc import ABC
from typing import Iterable, Mapping, Union

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
    ScrapeTaskOption,
    StatusReport,
)
from vigorish.util.list_helpers import report_dict


def same_value_for_all_data_sets_is_required(setting_name: str) -> bool:
    return setting_name in [
        "STATUS_REPORT",
        "S3_BUCKET",
        "SCRAPE_TASK_OPTION",
        "SCRAPED_DATA_COMBINE_CONDITION",
        "COMBINED_DATA_STORAGE",
        "COMBINED_DATA_LOCAL_FOLDER_PATH",
        "COMBINED_DATA_S3_FOLDER_PATH",
        "DB_BACKUP_FOLDER_PATH",
    ]


EnumSetting = Union[
    CombinedDataStorageOption,
    HtmlStorageOption,
    JsonStorageOption,
    ScrapeCondition,
    ScrapeTaskOption,
]
NumericSetting = Union[BatchJobSettings, BatchScrapeDelay, UrlScrapeDelay]
PathSetting = Union[LocalFolderPathSetting, S3FolderPathSetting]
ConfigValue = Union[None, bool, int, str, EnumSetting]
ConfigDict = Mapping[str, Union[None, bool, int, str]]
ConfigSettingValue = Union[EnumSetting, NumericSetting, PathSetting, str]


class ConfigSetting(ABC):
    data_type: ConfigType
    setting_name: str
    config_dict: ConfigDict

    def __init__(self, setting_name: str, config_dict: ConfigDict):
        self.setting_name = setting_name
        self.config_dict = config_dict
        self.data_type = ConfigType.NONE

    @property
    def setting_name_title(self) -> str:
        return " ".join(self.setting_name.split("_")).title()

    @property
    def description(self) -> str:
        return self.config_dict.get("DESCRIPTION", "")

    @property
    def possible_values(self) -> Iterable[EnumSetting]:
        return []

    @property
    def is_same_for_all_data_sets(self) -> bool:
        return (
            True
            if same_value_for_all_data_sets_is_required(self.setting_name)
            else self.config_dict.get("SAME_SETTING_FOR_ALL_DATA_SETS")
        )

    @property
    def current_settings_report(self) -> str:
        if self.is_same_for_all_data_sets:
            settings_dict = {"ALL_DATA_SETS": self.current_setting(DataSet.ALL)}
        else:
            settings_dict = {ds.name: self.current_setting(ds) for ds in DataSet}
        return report_dict(settings_dict, title="", title_prefix="", title_suffix="")

    def current_setting(self, data_set: DataSet) -> ConfigValue:
        return (
            self.config_dict.get(DataSet.ALL.name)
            if self.is_same_for_all_data_sets
            else self.config_dict.get(data_set.name)
        )


class StringConfigSetting(ConfigSetting):
    def __init__(self, setting_name: str, config_dict: ConfigDict):
        super().__init__(setting_name, config_dict)
        self.data_type = ConfigType.STRING

    def __repr__(self) -> str:
        return f"<StringConfigSetting setting={self.setting_name}, value={self.current_settings_report}>"


class PathConfigSetting(ConfigSetting):
    def __init__(self, setting_name: str, config_dict: ConfigDict):
        super().__init__(setting_name, config_dict)
        self.data_type = ConfigType.PATH

    def __repr__(self) -> str:
        return f"<PathConfigSetting setting={self.setting_name}, value={self.current_settings_report}>"

    def current_setting(self, data_set: DataSet) -> PathSetting:
        current_setting = super().current_setting(data_set)
        class_name = self.config_dict.get("CLASS_NAME")
        if class_name == "LocalFolderPathSetting":
            return LocalFolderPathSetting(path=current_setting, data_set=data_set)
        if class_name == "S3FolderPathSetting":
            return S3FolderPathSetting(path=current_setting, data_set=data_set)


class EnumConfigSetting(ConfigSetting):
    def __init__(self, setting_name: str, config_dict: ConfigDict):
        super().__init__(setting_name, config_dict)
        self.data_type = ConfigType.ENUM

    def __repr__(self) -> str:
        return f"<EnumConfigSetting setting={self.setting_name}, value={self.current_settings_report}>"

    @property
    def enum_name(self) -> str:
        return self.config_dict.get("ENUM_NAME")

    @property
    def possible_values(self) -> Iterable[EnumSetting]:
        if self.enum_name == "ScrapeCondition":
            return list(ScrapeCondition)
        if self.enum_name == "ScrapeTaskOption":
            return list(ScrapeTaskOption)
        if self.enum_name == "HtmlStorageOption":
            return list(HtmlStorageOption)
        if self.enum_name == "JsonStorageOption":
            return list(JsonStorageOption)
        if self.enum_name == "CombinedDataStorageOption":
            return list(CombinedDataStorageOption)
        if self.enum_name == "StatusReport":
            return list(StatusReport)
        return []

    def current_setting(self, data_set: DataSet) -> EnumSetting:
        current_setting = super().current_setting(data_set).upper()
        if self.enum_name == "ScrapeCondition":
            return ScrapeCondition[current_setting]
        if self.enum_name == "ScrapeTaskOption":
            return ScrapeTaskOption[current_setting]
        if self.enum_name == "HtmlStorageOption":
            return HtmlStorageOption[current_setting]
        if self.enum_name == "JsonStorageOption":
            return JsonStorageOption[current_setting]
        if self.enum_name == "CombinedDataStorageOption":
            return CombinedDataStorageOption[current_setting]
        if self.enum_name == "StatusReport":
            return StatusReport[current_setting]
        return None


class NumericConfigSetting(ConfigSetting):
    def __init__(self, setting_name: str, config_dict: ConfigDict):
        super().__init__(setting_name, config_dict)
        self.data_type = ConfigType.NUMERIC

    def __repr__(self) -> str:
        return f"<NumericConfigSetting setting={self.setting_name}, value={self.current_settings_report}>"

    def current_setting(self, data_set: DataSet) -> NumericSetting:
        current_setting = super().current_setting(data_set)
        class_name = self.config_dict.get("CLASS_NAME")
        if class_name == "UrlScrapeDelay":
            return UrlScrapeDelay.from_config(current_setting)
        if class_name == "BatchJobSettings":
            return BatchJobSettings.from_config(current_setting)
        if class_name == "BatchScrapeDelay":
            return BatchScrapeDelay.from_config(current_setting)
        return None
