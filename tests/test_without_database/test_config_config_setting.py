from pathlib import Path

from tests.conftest import DOTENV_FILE, TESTS_FOLDER
from vigorish.config.config_file import ConfigFile
from vigorish.config.config_setting import (
    EnumConfigSetting,
    NumericConfigSetting,
    PathConfigSetting,
    StringConfigSetting,
)
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


def test_config_setting():
    default_config_path = Path(TESTS_FOLDER).joinpath("default.config.json")
    if default_config_path.exists():
        default_config_path.unlink()

    config_file = ConfigFile(config_file_path=default_config_path)
    config_file.create_default_config_file()
    assert default_config_path.exists()

    enum_setting_name = "STATUS_REPORT"
    enum_config_dict = config_file.config_json.get(enum_setting_name)
    enum_setting = EnumConfigSetting(enum_setting_name, enum_config_dict)
    assert enum_setting.setting_name_title == "Status Report"
    assert enum_setting.data_type == ConfigType.ENUM
    assert enum_setting.description
    assert enum_setting.possible_values == list(StatusReport)
    assert enum_setting.current_setting(DataSet.ALL) == StatusReport.SEASON_SUMMARY
    assert "ALL_DATA_SETS..: SEASON_SUMMARY" in enum_setting.current_settings_report

    default_enum_settings = {
        "SCRAPE_CONDITION": {"enum_type": ScrapeCondition, "default_value": ScrapeCondition.ONLY_MISSING_DATA},
        "SCRAPE_TASK_OPTION": {"enum_type": ScrapeTaskOption, "default_value": ScrapeTaskOption.BY_DATE},
        "HTML_STORAGE": {"enum_type": HtmlStorageOption, "default_value": HtmlStorageOption.NONE},
        "JSON_STORAGE": {"enum_type": JsonStorageOption, "default_value": JsonStorageOption.LOCAL_FOLDER},
        "COMBINED_DATA_STORAGE": {
            "enum_type": CombinedDataStorageOption,
            "default_value": CombinedDataStorageOption.LOCAL_FOLDER,
        },
    }
    for setting_name, setting_dict in default_enum_settings.items():
        config_dict = config_file.config_json.get(setting_name)
        setting = EnumConfigSetting(setting_name, config_dict)
        assert setting.possible_values == list(setting_dict["enum_type"])
        assert setting.current_setting(DataSet.BROOKS_PITCHFX) == setting_dict["default_value"]

    str_setting_name = "S3_BUCKET"
    str_config_dict = config_file.config_json.get(str_setting_name)
    str_setting = StringConfigSetting(str_setting_name, str_config_dict)
    assert str_setting.setting_name_title == "S3 Bucket"
    assert str_setting.data_type == ConfigType.STRING
    assert str_setting.description
    assert not str_setting.possible_values
    assert str_setting.current_setting(DataSet.ALL) == "your-bucket"

    num_setting_name = "URL_SCRAPE_DELAY"
    num_config_dict = config_file.config_json.get(num_setting_name)
    num_setting = NumericConfigSetting(num_setting_name, num_config_dict)
    assert num_setting.setting_name_title == "Url Scrape Delay"
    assert num_setting.data_type == ConfigType.NUMERIC
    assert num_setting.description
    assert not num_setting.possible_values
    assert num_setting.current_setting(DataSet.BBREF_GAMES_FOR_DATE) == UrlScrapeDelay(True, True, None, 3, 6)

    path_setting_name = "JSON_S3_FOLDER_PATH"
    path_config_dict = config_file.config_json.get(path_setting_name)
    path_setting = PathConfigSetting(path_setting_name, path_config_dict)
    assert path_setting.setting_name_title == "Json S3 Folder Path"
    assert path_setting.data_type == ConfigType.PATH
    assert path_setting.description
    assert not path_setting.possible_values
    check_data_set = DataSet.BROOKS_GAMES_FOR_DATE
    current_setting = path_setting.current_setting(check_data_set)
    check_setting = S3FolderPathSetting(path_config_dict.get(DataSet.ALL.name), check_data_set)
    assert current_setting.resolve(2019) == check_setting.resolve(2019)

    path_setting_name = "HTML_LOCAL_FOLDER_PATH"
    path_config_dict = config_file.config_json.get(path_setting_name)
    path_setting = PathConfigSetting(path_setting_name, path_config_dict)
    assert path_setting.setting_name_title == "Html Local Folder Path"
    assert path_setting.data_type == ConfigType.PATH
    assert path_setting.description
    assert not path_setting.possible_values
    check_data_set = DataSet.BBREF_BOXSCORES
    current_setting = path_setting.current_setting(check_data_set)
    check_setting = LocalFolderPathSetting(path_config_dict.get(DataSet.ALL.name), check_data_set)
    assert str(check_setting) == path_config_dict.get(DataSet.ALL.name)
    assert check_setting.is_valid(2019)
    assert current_setting.resolve(2019) == check_setting.resolve(2019)

    default_config_path.unlink()
    if DOTENV_FILE.exists():
        DOTENV_FILE.unlink()
