from abc import ABC, abstractmethod
from pathlib import Path

from vigorish.util.numeric_helpers import validate_year_value
from vigorish.util.sys_helpers import validate_folder_path

YEAR_TOKEN = "{year}"
DATA_SET_TOKEN = "{data_set}"


class FolderPathSetting(ABC):
    _path_str: str

    def __init__(self, path, data_set):
        self._path_str = path
        self.data_set = data_set

    def __str__(self):
        return self._path_str

    @abstractmethod
    def resolve(self, year=None):
        pass


class S3FolderPathSetting(FolderPathSetting):
    def resolve(self, year=None):
        path_str = self._path_str
        if YEAR_TOKEN in self._path_str:
            validate_year_value(year)
            if isinstance(year, int):
                year = str(year)
            path_str = path_str.replace(YEAR_TOKEN, year)
        if DATA_SET_TOKEN in self._path_str:
            path_str = path_str.replace(DATA_SET_TOKEN, self.data_set.name.lower())
        return path_str


class LocalFolderPathSetting(FolderPathSetting):
    def is_valid(self, year):
        absolute_path = self.resolve(year)
        return validate_folder_path(absolute_path)

    def resolve(self, year: None):
        path_str = self._path_str
        if YEAR_TOKEN in self._path_str:
            validate_year_value(year)
            if isinstance(year, int):
                year = str(year)
            path_str = path_str.replace(YEAR_TOKEN, year)
        if DATA_SET_TOKEN in self._path_str:
            path_str = path_str.replace(DATA_SET_TOKEN, self.data_set.name.lower())
        return str(Path(path_str).resolve())
