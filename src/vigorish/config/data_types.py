"""Enum and Dataclass definitions."""
from dataclasses import dataclass
from enum import Enum, auto


@dataclass
class UrlScrapeDelaySettings:
    delay_required: bool
    delay_is_random: bool
    delay_same_ms: int
    delay_random_min_ms: int
    delay_random_max_ms: int


@dataclass
class BatchJobSettings:
    batched_scraping_enabled: bool
    batch_size_is_random: bool
    batch_size_same: int
    batch_size_random_min: int
    batch_size_random_max: int


@dataclass
class BatchScrapeDelaySettings:
    delay_required: bool
    delay_is_random: bool
    delay_same_ms: int
    delay_random_min_ms: int
    delay_random_max_ms: int


class ScrapeOption(Enum):
    """Allowed values for SCRAPE_DATA_SET config setting."""

    ONLY_MISSING_DATA = auto()
    ALWAYS = auto()
    NEVER = auto()

    def __str__(self):
        return self.name


class UpdateOption(Enum):
    """Allowed values for UPDATE_DATA_SET config setting."""

    ONLY_MISSING_DATA = auto()
    ALWAYS = auto()
    NEVER = auto()

    def __str__(self):
        return self.name


class HtmlStorage(Enum):
    """Allowed values for HTML_STORAGE config setting."""

    LOCAL_FOLDER = auto()
    S3_BUCKET = auto()
    BOTH = auto()

    def __str__(self):
        return self.name


class JsonStorage(Enum):
    """Allowed values for JSON_STORAGE config setting."""

    LOCAL_FOLDER = auto()
    S3_BUCKET = auto()
    BOTH = auto()

    def __str__(self):
        kk


class ScrapeTool(Enum):
    """Config setting for scrape tool: requests/selenium or nightmarejs."""

    REQUESTS_SELENIUM = auth()
    NIGHTMAREJS = auto()

    def __str__(self):
        return self.name


class StatusReport(Enum):
    """The type of status report (if any) to display after data has been scraped."""

    SEASON_SUMMARY = auto()
    DATE_SUMMARY = auto()
    DATE_DETAIL = auto()
    NONE = auto()

    def __str__(self):
        return self.name
