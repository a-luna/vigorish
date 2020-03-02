"""Class definitions for settings with numeric value that can either be constant or random."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Optional, Tuple

from vigorish.config.typing import NUMERIC_OPTIONS_JSON_VALUE


@dataclass
class UrlScrapeDelaySettings:
    delay_is_required: Optional[bool]
    delay_is_random: Optional[bool]
    delay_uniform: Optional[int]
    delay_random_min: Optional[int]
    delay_random_max: Optional[int]

    def __str__(self) -> str:
        if not self.delay_is_required:
            return "No delay after each URL"
        if not self.delay_is_random:
            return f"Delay is uniform ({self.delay_uniform} seconds)"
        return f"Delay is random ({self.delay_random_min}-{self.delay_random_max} seconds)"

    @property
    def delay_uniform_ms(self) -> int:
        return self.delay_uniform * 1000 if self.delay_uniform else 0

    @property
    def delay_random_min_ms(self) -> int:
        return self.delay_random_min * 1000 if self.delay_random_min else 0

    @property
    def delay_random_max_ms(self) -> int:
        return self.delay_random_max * 1000 if self.delay_random_max else 0

    def to_dict(self) -> NUMERIC_OPTIONS_JSON_VALUE:
        return {
            "URL_SCRAPE_DELAY_IS_REQUIRED": self.delay_is_required,
            "URL_SCRAPE_DELAY_IS_RANDOM": self.delay_is_random,
            "URL_SCRAPE_DELAY_IN_SECONDS": self.delay_uniform,
            "URL_SCRAPE_DELAY_IN_SECONDS_MIN": self.delay_random_min,
            "URL_SCRAPE_DELAY_IN_SECONDS_MAX": self.delay_random_max,
        }

    @staticmethod
    def from_config(config_dict: NUMERIC_OPTIONS_JSON_VALUE) -> UrlScrapeDelaySettings:
        delay_is_required = config_dict.get("URL_SCRAPE_DELAY_IS_REQUIRED", True)
        delay_is_random = config_dict.get("URL_SCRAPE_DELAY_IS_RANDOM", True)
        delay_uniform = config_dict.get("URL_SCRAPE_DELAY_IN_SECONDS", 0)
        delay_random_min = config_dict.get("URL_SCRAPE_DELAY_IN_SECONDS_MIN", 3)
        delay_random_max = config_dict.get("URL_SCRAPE_DELAY_IN_SECONDS_MAX", 6)
        return UrlScrapeDelaySettings(
            delay_is_required=delay_is_required,
            delay_is_random=delay_is_random,
            delay_uniform=delay_uniform,
            delay_random_min=delay_random_min,
            delay_random_max=delay_random_max,
        )

    @staticmethod
    def from_tuple(new_value: Tuple[bool, bool, int, int, int]) -> UrlScrapeDelaySettings:
        is_required, is_random, uniform_value, random_min, random_max = new_value
        return UrlScrapeDelaySettings(
            delay_is_required=is_required,
            delay_is_random=is_random,
            delay_uniform=uniform_value,
            delay_random_min=random_min,
            delay_random_max=random_max,
        )

    @staticmethod
    def null_object() -> Mapping[str, None]:
        return {
            "URL_SCRAPE_DELAY_IS_REQUIRED": None,
            "URL_SCRAPE_DELAY_IS_RANDOM": None,
            "URL_SCRAPE_DELAY_IN_SECONDS": None,
            "URL_SCRAPE_DELAY_IN_SECONDS_MIN": None,
            "URL_SCRAPE_DELAY_IN_SECONDS_MAX": None,
        }


@dataclass
class BatchJobSettings:
    batched_scraping_enabled: Optional[bool]
    batch_size_is_random: Optional[bool]
    batch_size_uniform: Optional[int]
    batch_size_random_min: Optional[int]
    batch_size_random_max: Optional[int]

    def __str__(self) -> str:
        if not self.batched_scraping_enabled:
            return "Batched scraping is not enabled"
        if not self.batch_size_is_random:
            return f"Batch size is uniform ({self.batch_size_uniform} URLs)"
        return f"Batch size is random ({self.batch_size_random_min}-{self.batch_size_random_max} URLs)"

    def to_dict(self) -> NUMERIC_OPTIONS_JSON_VALUE:
        return {
            "CREATE_BATCHED_SCRAPE_JOBS": self.batched_scraping_enabled,
            "USE_IRREGULAR_BATCH_SIZES": self.batch_size_is_random,
            "BATCH_SIZE": self.batch_size_uniform,
            "BATCH_SIZE_MIN": self.batch_size_random_min,
            "BATCH_SIZE_MAX": self.batch_size_random_max,
        }

    @staticmethod
    def from_config(config_dict: NUMERIC_OPTIONS_JSON_VALUE) -> BatchJobSettings:
        batched_scraping_enabled = config_dict.get("CREATE_BATCHED_SCRAPE_JOBS", True)
        batch_size_is_random = config_dict.get("USE_IRREGULAR_BATCH_SIZES", True)
        batch_size_uniform = config_dict.get("BATCH_SIZE", True)
        batch_size_random_min = config_dict.get("BATCH_SIZE_MIN", True)
        batch_size_random_max = config_dict.get("BATCH_SIZE_MAX", True)
        return BatchJobSettings(
            batched_scraping_enabled=batched_scraping_enabled,
            batch_size_is_random=batch_size_is_random,
            batch_size_uniform=batch_size_uniform,
            batch_size_random_min=batch_size_random_min,
            batch_size_random_max=batch_size_random_max,
        )

    @staticmethod
    def from_tuple(new_value: Tuple[bool, bool, int, int, int]) -> BatchJobSettings:
        is_required, is_random, uniform_value, random_min, random_max = new_value
        return BatchJobSettings(
            batched_scraping_enabled=is_required,
            batch_size_is_random=is_random,
            batch_size_uniform=uniform_value,
            batch_size_random_min=random_min,
            batch_size_random_max=random_max,
        )

    @staticmethod
    def null_object() -> Mapping[str, None]:
        return {
            "CREATE_BATCHED_SCRAPE_JOBS": None,
            "USE_IRREGULAR_BATCH_SIZES": None,
            "BATCH_SIZE": None,
            "BATCH_SIZE_MIN": None,
            "BATCH_SIZE_MAX": None,
        }


@dataclass
class BatchScrapeDelaySettings:
    delay_is_required: Optional[bool]
    delay_is_random: Optional[bool]
    delay_uniform: Optional[int]
    delay_random_min: Optional[int]
    delay_random_max: Optional[int]

    def __str__(self) -> str:
        if not self.delay_is_required:
            return "No delay after each batch"
        if not self.delay_is_random:
            return f"Delay is uniform ({self.delay_uniform} minutes)"
        return f"Delay is random ({self.delay_random_min}-{self.delay_random_max} minutes)"

    @property
    def delay_uniform_ms(self) -> int:
        return self.delay_uniform * 60 * 1000 if self.delay_uniform else 0

    @property
    def delay_random_min_ms(self) -> int:
        return self.delay_random_min * 60 * 1000 if self.delay_random_min else 0

    @property
    def delay_random_max_ms(self) -> int:
        return self.delay_random_max * 60 * 1000 if self.delay_random_max else 0

    def to_dict(self) -> NUMERIC_OPTIONS_JSON_VALUE:
        return {
            "BATCH_SCRAPE_DELAY_IS_REQUIRED": self.delay_is_required,
            "BATCH_SCRAPE_DELAY_IS_RANDOM": self.delay_is_random,
            "BATCH_SCRAPE_DELAY_IN_MINUTES": self.delay_uniform,
            "BATCH_SCRAPE_DELAY_IN_MINUTES_MIN": self.delay_random_min,
            "BATCH_SCRAPE_DELAY_IN_MINUTES_MAX": self.delay_random_max,
        }

    @staticmethod
    def from_config(config_dict: NUMERIC_OPTIONS_JSON_VALUE) -> BatchScrapeDelaySettings:
        delay_is_required = config_dict.get("BATCH_SCRAPE_DELAY_IS_REQUIRED", True)
        delay_is_random = config_dict.get("BATCH_SCRAPE_DELAY_IS_RANDOM", True)
        delay_uniform = config_dict.get("BATCH_SCRAPE_DELAY_IN_MINUTES", 0)
        delay_random_min = config_dict.get("BATCH_SCRAPE_DELAY_IN_MINUTES_MIN", 5)
        delay_random_max = config_dict.get("BATCH_SCRAPE_DELAY_IN_MINUTES_MAX", 10)
        return BatchScrapeDelaySettings(
            delay_is_required=delay_is_required,
            delay_is_random=delay_is_random,
            delay_uniform=delay_uniform,
            delay_random_min=delay_random_min,
            delay_random_max=delay_random_max,
        )

    @staticmethod
    def from_tuple(new_value: Tuple[bool, bool, int, int, int]) -> BatchScrapeDelaySettings:
        is_required, is_random, uniform_value, random_min, random_max = new_value
        return BatchScrapeDelaySettings(
            delay_is_required=is_required,
            delay_is_random=is_random,
            delay_uniform=uniform_value,
            delay_random_min=random_min,
            delay_random_max=random_max,
        )

    @staticmethod
    def null_object() -> Mapping[str, None]:
        return {
            "BATCH_SCRAPE_DELAY_IS_REQUIRED": None,
            "BATCH_SCRAPE_DELAY_IS_RANDOM": None,
            "BATCH_SCRAPE_DELAY_IN_MINUTES": None,
            "BATCH_SCRAPE_DELAY_IN_MINUTES_MIN": None,
            "BATCH_SCRAPE_DELAY_IN_MINUTES_MAX": None,
        }
