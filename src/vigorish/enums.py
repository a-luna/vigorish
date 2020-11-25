"""Enum definitions."""
from enum import Enum, IntEnum

from aenum import auto, IntFlag


class VigFile(IntFlag):
    """File types that are scraped, created and stored by vigorish."""

    SCRAPED_HTML = auto()
    PARSED_JSON = auto()
    COMBINED_GAME_DATA = auto()
    PATCH_LIST = auto()
    ALL = SCRAPED_HTML | PARSED_JSON | COMBINED_GAME_DATA | PATCH_LIST

    def __str__(self):
        return self.name

    @classmethod
    def from_str(cls, file_type_name):
        for file_type in cls:
            if file_type_name.upper() not in file_type.name:
                continue
            return file_type


class DataSet(IntFlag):
    """MLB data sets."""

    BBREF_GAMES_FOR_DATE = auto()
    BROOKS_GAMES_FOR_DATE = auto()
    BBREF_BOXSCORES = auto()
    BROOKS_PITCH_LOGS = auto()
    BROOKS_PITCHFX = auto()
    ALL = (
        BBREF_GAMES_FOR_DATE
        | BROOKS_GAMES_FOR_DATE
        | BBREF_BOXSCORES
        | BROOKS_PITCH_LOGS
        | BROOKS_PITCHFX
    )

    def __str__(self):
        return self.name

    @classmethod
    def from_str(cls, data_set_name):
        for data_set in cls:
            if data_set_name.upper() not in data_set.name:
                continue
            return data_set


class ConfigType(str, Enum):
    """Data types for configuration settings."""

    STRING = "string"
    NUMERIC = "numeric"
    PATH = "path"
    ENUM = "enum"
    NONE = "none"

    def __str__(self):
        return str.__str__(self)


class ScrapeCondition(str, Enum):
    """Allowed values for SCRAPE_CONDITION config setting."""

    ONLY_MISSING_DATA = "only_missing_data"
    ALWAYS = "always"
    NEVER = "never"

    def __str__(self):
        return str.__str__(self)


class ScrapeTaskOption(str, Enum):
    """Allowed values for SCRAPE_TASK_OPTION config setting."""

    BY_DATE = "by_date"
    BY_DATA_SET = "by_data_set"

    def __str__(self):
        return str.__str__(self)


class HtmlStorageOption(str, Enum):
    """Allowed values for HTML_STORAGE config setting."""

    NONE = "none"
    LOCAL_FOLDER = "local_folder"
    S3_BUCKET = "s3_bucket"
    BOTH = "both"

    def __str__(self):
        return str.__str__(self)


class JsonStorageOption(str, Enum):
    """Allowed values for JSON_STORAGE config setting."""

    LOCAL_FOLDER = "local_folder"
    S3_BUCKET = "s3_bucket"
    BOTH = "both"

    def __str__(self):
        return str.__str__(self)


class CombinedDataStorageOption(str, Enum):
    """Allowed values for COMBINED_DATA_STORAGE config setting."""

    LOCAL_FOLDER = "local_folder"
    S3_BUCKET = "s3_bucket"
    BOTH = "both"

    def __str__(self):
        return str.__str__(self)


class StatusReport(IntEnum):
    """The type of status report (if any) to display."""

    NONE = 0
    SEASON_SUMMARY = 1
    DATE_SUMMARY_MISSING_DATA = 2
    DATE_SUMMARY_ALL_DATES = 3
    DATE_DETAIL_MISSING_DATA = 4
    DATE_DETAIL_ALL_DATES = 5
    DATE_DETAIL_MISSING_PITCHFX = 6
    SINGLE_DATE_WITH_GAME_STATUS = 7

    def __str__(self):
        return self.name


class FileTask(str, Enum):
    """Generic file actions that are mapped to LocalFileTask and S3FileTask file actions."""

    STORE_FILE = "store_file"
    GET_FILE = "get_file"
    REMOVE_FILE = "remove_file"

    def __str__(self):
        return str.__str__(self)


class S3FileTask(str, Enum):
    """Type of action to perform on an object in an S3 bucket."""

    UPLOAD = "upload"
    DOWNLOAD = "download"
    DELETE = "delete"
    RENAME = "rename"

    def __str__(self):
        return str.__str__(self)


class LocalFileTask(str, Enum):
    """Type of action to perform on a local file."""

    WRITE_FILE = "write_file"
    READ_FILE = "read_file"
    DELETE_FILE = "delete_file"
    DECODE_JSON = "decode_file"

    def __str__(self):
        return str.__str__(self)


class JobStatus(IntEnum):
    """Status of a scrape job created by a user."""

    INCOMPLETE = 1
    ERROR = 2
    COMPLETE = 3

    def __str__(self):
        return self.name


class DefensePosition(IntEnum):
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
            "NONE": "BN",
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

    @classmethod
    def from_abbrev(cls, abbrev):
        abbrev_dict = {
            "P": cls.PITCHER,
            "C": cls.CATCHER,
            "1B": cls.FIRST_BASE,
            "2B": cls.SECOND_BASE,
            "3B": cls.THIRD_BASE,
            "SS": cls.SHORT_STOP,
            "LF": cls.LEFT_FIELD,
            "CF": cls.CENTER_FIELD,
            "RF": cls.RIGHT_FIELD,
            "DH": cls.DH,
        }
        return abbrev_dict.get(abbrev, cls.NONE)


class PitchType(IntFlag):
    CHANGEUP = auto()
    CURVEBALL = auto()
    EEPHUS = auto()
    FASTBALL = auto()
    CUTTER = auto()
    FOUR_SEAM_FASTBALL = auto()
    SPLITTER = auto()
    TWO_SEAM_FASTBALL = auto()
    FORKBALL = auto()
    INTENT_BALL = auto()
    KNUCKLE_BALL_CURVE = auto()
    KNUCKLE_BALL = auto()
    PITCH_OUT = auto()
    SCREWBALL = auto()
    SINKER = auto()
    SLIDER = auto()
    UNKNOWN = auto()
    ALL = (
        CHANGEUP
        | CURVEBALL
        | EEPHUS
        | FASTBALL
        | CUTTER
        | FOUR_SEAM_FASTBALL
        | SPLITTER
        | TWO_SEAM_FASTBALL
        | FORKBALL
        | INTENT_BALL
        | KNUCKLE_BALL_CURVE
        | KNUCKLE_BALL
        | PITCH_OUT
        | SCREWBALL
        | SINKER
        | SLIDER
        | UNKNOWN
    )

    def __str__(self):
        abbrev_dict = {
            "CHANGEUP": "CH",
            "CURVEBALL": "CU",
            "EEPHUS": "EP",
            "FASTBALL": "FA",
            "CUTTER": "FC",
            "FOUR_SEAM_FASTBALL": "FF",
            "SPLITTER": "FS",
            "TWO_SEAM_FASTBALL": "FT",
            "FORKBALL": "FO",
            "INTENT_BALL": "IN",
            "KNUCKLE_BALL_CURVE": "KC",
            "KNUCKLE_BALL": "KN",
            "PITCH_OUT": "PO",
            "SCREWBALL": "SC",
            "SINKER": "SI",
            "SLIDER": "SL",
            "UNKNOWN": "UN",
        }
        return abbrev_dict.get(self.name, self.name)

    @property
    def print_name(self):
        name_dict = {
            "FF": "Four-seam Fastball",
            "FT": "Two-seam Fastball",
            "IN": "Intent ball",
            "KC": "Knuckle ball Curve",
            "KN": "Knuckle ball",
        }
        return name_dict.get(str(self), self.name.replace("_", " ").title())

    @classmethod
    def from_abbrev(cls, abbrev):
        abbrev_dict = {
            "CH": cls.CHANGEUP,
            "CU": cls.CURVEBALL,
            "EP": cls.EEPHUS,
            "FA": cls.FASTBALL,
            "FC": cls.CUTTER,
            "FF": cls.FOUR_SEAM_FASTBALL,
            "FS": cls.SPLITTER,
            "FT": cls.TWO_SEAM_FASTBALL,
            "FO": cls.FORKBALL,
            "IN": cls.INTENT_BALL,
            "KC": cls.KNUCKLE_BALL_CURVE,
            "KN": cls.KNUCKLE_BALL,
            "PO": cls.PITCH_OUT,
            "SC": cls.SCREWBALL,
            "SI": cls.SINKER,
            "SL": cls.SLIDER,
            "UN": cls.UNKNOWN,
        }
        return abbrev_dict.get(abbrev, cls.UNKNOWN)


class SeasonType(str, Enum):
    """Identifies games, stats, etc as Spring Training, Regular, or Post-Season."""

    PRE_SEASON = "pre_season"
    REGULAR_SEASON = "regular_season"
    POST_SEASON = "post_season"
    NONE = "none"

    def __str__(self):
        season_type_dict = {
            "PRE_SEASON": "Spring Training",
            "REGULAR_SEASON": "Regular Season",
            "POST_SEASON": "Post-Season",
        }
        return season_type_dict.get(self.name, self.name)


class PlayByPlayEvent(str, Enum):
    AT_BAT = "at_bat"
    SUBSTITUTION = "substitution"
    MISC = "misc"

    def __str__(self):
        return str.__str__(self)


class AuditError(str, Enum):
    FAILED_TO_COMBINE = "failed_to_combine"
    PITCHFX_ERROR = "pitchfx_error"
    INVALID_PITCHFX_DATA = "invalid_pitchfx_data"

    def __str__(self):
        error_type_dict = {
            "FAILED_TO_COMBINE": "Failed to combine scraped data",
            "PITCHFX_ERROR": "PitchFX error",
            "INVALID_PITCHFX_DATA": "Invalid PitchFX data",
        }
        return error_type_dict.get(self.name, self.name)


class AuditTask(IntEnum):
    GET_SCRAPED_DATA = 1
    GET_PLAY_BY_PLAY_DATA = 2
    GET_PITCHFX_DATA = 3
    COMBINE_PBP_AND_PFX_DATA = 4
    UPDATE_BOXSCORE = 5

    def __str__(self):
        return self.name


class PatchType(str, Enum):
    CHANGE_BBREF_GAME_ID = "change_bbref_game_id"
    CHANGE_PITCH_SEQUENCE = "change_pitch_sequence"
    CHANGE_BATTER_ID = "change_batter_id"
    CHANGE_PITCHER_ID = "change_pitcher_id"

    def __str__(self):
        return str.__str__(self)


class SyncDirection(str, Enum):
    UP_TO_S3 = "up_to_s3"
    DOWN_TO_LOCAL = "down_to_local"

    def __str__(self):
        return str.__str__(self)
