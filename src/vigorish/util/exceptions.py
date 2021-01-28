"""Custom exceptions for issues specific to vigorish actions/processes."""
from vigorish.enums import DataSet


class ConfigSettingException(Exception):
    """Exception raised when an unrecoverable error occurs due to a ConfigSetting."""

    def __init__(self, setting_name, data_set=DataSet.ALL, current_value=None, detail=None):
        message = f"ConfigSetting value is invalid:\n\tName...........: {setting_name} (Data Set: {data_set})\n"
        if current_value:
            message += f"\tCurrent Value..: {current_value}\n"
        if detail:
            message += f"\tDetail.........: {detail}\n"
        super().__init__(message)


class ScrapedDataException(Exception):
    """Exception raised when data identified by file_type, data_set and url_id cannot be found."""

    def __init__(self, file_type, data_set, url_id):
        message = f"Failed to locate scraped data: URL ID: {url_id} (File Type: {file_type}, Data Set: {data_set})"
        super().__init__(message)
