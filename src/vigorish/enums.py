"""Enum definitions."""
from enum import Enum, IntEnum, auto

from aenum import IntFlag
from aenum import auto as auto_flag


class DataSet(IntFlag):
    """MLB data sets."""

    BBREF_GAMES_FOR_DATE = auto_flag()
    BROOKS_GAMES_FOR_DATE = auto_flag()
    BBREF_BOXSCORES = auto_flag()
    BROOKS_PITCH_LOGS = auto_flag()
    BROOKS_PITCHFX = auto_flag()
    ALL = (
        BBREF_GAMES_FOR_DATE
        | BROOKS_GAMES_FOR_DATE
        | BBREF_BOXSCORES
        | BROOKS_PITCH_LOGS
        | BROOKS_PITCHFX
    )

    def __str__(self):
        return self.name


class ConfigType(Enum):
    """Data types for configuration settings."""

    STRING = auto()
    NUMERIC = auto()
    PATH = auto()
    ENUM = auto()
    NONE = auto()

    def __str__(self):
        return self.name


class ScrapeCondition(Enum):
    """Allowed values for SCRAPE_CONDITION config setting."""

    ONLY_MISSING_DATA = auto()
    ALWAYS = auto()
    NEVER = auto()

    def __str__(self):
        return self.name


class HtmlStorageOption(Enum):
    """Allowed values for HTML_STORAGE config setting."""

    LOCAL_FOLDER = auto()
    S3_BUCKET = auto()
    BOTH = auto()

    def __str__(self):
        return self.name


class JsonStorageOption(Enum):
    """Allowed values for JSON_STORAGE config setting."""

    LOCAL_FOLDER = auto()
    S3_BUCKET = auto()
    BOTH = auto()

    def __str__(self):
        return self.name


class StatusReport(Enum):
    """The type of status report (if any) to display."""

    NONE = 0
    SEASON_SUMMARY = 1
    DATE_SUMMARY_MISSING_DATA = 2
    DATE_SUMMARY_ALL_DATES = 3
    DATE_DETAIL_MISSING_DATA = 4
    DATE_DETAIL_ALL_DATES = 5
    DATE_DETAIL_MISSING_PITCHFX = 6

    def __str__(self):
        return self.name


class FileTask(Enum):
    """Generic file actions that are mapped to LocalFileTask and S3FileTask file actions."""

    STORE_FILE = auto()
    GET_FILE = auto()
    REMOVE_FILE = auto()

    def __str__(self):
        return self.name


class S3FileTask(Enum):
    """Type of action to perform on an object in an S3 bucket."""

    UPLOAD = auto()
    DOWNLOAD = auto()
    DELETE = auto()
    RENAME = auto()

    def __str__(self):
        return self.name


class LocalFileTask(Enum):
    """Type of action to perform on a local file."""

    WRITE_FILE = auto()
    READ_FILE = auto()
    DELETE_FILE = auto()
    DECODE_JSON = auto()

    def __str__(self):
        return self.name


class DocFormat(Enum):
    """Document formats scraped, created and stored by this program."""

    JSON = auto()
    HTML = auto()

    def __str__(self):
        return self.name


class JobStatus(IntEnum):
    """Status of a scrape job created by a user."""

    INCOMPLETE = auto()
    ERROR = auto()
    COMPLETE = auto()

    def __str__(self):
        return self.name


class DefensePosition(Enum):
    """Defensive positions and position numbers used when scoring a game."""

    NONE = 0
    PITCHER = 1
    CATCHER = 2
    FIRST_BASE = 3
    SECOND_BASE = 4
    THIRD_BASE = 5
    SHORT_STOP = 6
    LEFT_FIELD = 7
    CENTER_FIELD = 8
    RIGHT_FIELD = 9
    DH = 10

    def __str__(self):
        abbrev_dict = {
            "NONE": "N/A",
            "PITCHER": "P",
            "CATCHER": "C",
            "FIRST_BASE": "1B",
            "SECOND_BASE": "2B",
            "THIRD_BASE": "3B",
            "SHORT_STOP": "SS",
            "LEFT_FIELD": "LF",
            "CENTER_FIELD": "CF",
            "RIGHT_FIELD": "RF",
        }
        return abbrev_dict.get(self.name, self.name)


class SeasonType(Enum):
    """Identifies games, stats, etc as Spring Training, Regular, or Post-Season."""

    PRE_SEASON = auto()
    REGULAR_SEASON = auto()
    POST_SEASON = auto()
    NONE = auto()

    def __str__(self):
        season_type_dict = {
            "PRE_SEASON": "Spring Training",
            "REGULAR_SEASON": "Regular Season",
            "POST_SEASON": "Post-Season",
        }
        return season_type_dict.get(self.name, self.name)


class PlayByPlayEvent(Enum):
    AT_BAT = auto()
    SUBSTITUTION = auto()
    MISC = auto()

    def __str__(self):
        return self.name
