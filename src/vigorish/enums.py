"""Enum definitions."""
from enum import Enum, auto


class DataSet(Enum):
    """MLB data sets."""

    NONE = auto()
    BBREF_GAMES_FOR_DATE = auto()
    BBREF_BOXSCORES = auto()
    BROOKS_GAMES_FOR_DATE = auto()
    BROOKS_PITCH_LOGS = auto()
    BROOKS_PITCHFX = auto()


class ScrapeCondition(Enum):
    """Allowed values for SCRAPE_CONDITION config setting."""

    ONLY_MISSING_DATA = auto()
    ALWAYS = auto()
    NEVER = auto()


class ConfigDataType(Enum):
    """Data types for configuration settings."""

    NONE = auto()
    STRING = auto()
    ENUM = auto()
    NUMERIC = auto()


class HtmlStorage(Enum):
    """Allowed values for HTML_STORAGE config setting."""

    NONE = auto()
    LOCAL_FOLDER = auto()
    S3_BUCKET = auto()
    BOTH = auto()


class JsonStorage(Enum):
    """Allowed values for JSON_STORAGE config setting."""

    LOCAL_FOLDER = auto()
    S3_BUCKET = auto()
    BOTH = auto()


class ScrapeTool(Enum):
    """Config setting for scrape tool: requests/selenium or nightmarejs."""

    REQUESTS_SELENIUM = auto()
    NIGHTMAREJS = auto()


class StatusReport(Enum):
    """The type of status report (if any) to display after data has been scraped."""

    NONE = auto()
    SEASON_SUMMARY = auto()
    DATE_SUMMARY = auto()
    DATE_DETAIL = auto()
