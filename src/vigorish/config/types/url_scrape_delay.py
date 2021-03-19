from dataclasses import dataclass


@dataclass
class UrlScrapeDelay:
    delay_is_required: bool
    delay_is_random: bool
    delay_uniform: int
    delay_random_min: int
    delay_random_max: int

    def __str__(self):
        if not self.delay_is_required:
            return "No delay after each URL"
        if not self.delay_is_random:
            return f"Delay is uniform ({self.delay_uniform} seconds)"
        return f"Delay is random ({self.delay_random_min}-{self.delay_random_max} seconds)"

    @property
    def delay_uniform_ms(self):
        return self.delay_uniform * 1000 if self.delay_uniform else 0

    @property
    def delay_random_min_ms(self):
        return self.delay_random_min * 1000 if self.delay_random_min else 0

    @property
    def delay_random_max_ms(self):
        return self.delay_random_max * 1000 if self.delay_random_max else 0

    @property
    def is_valid(self):
        if not self.delay_is_required:
            return False
        if not self.delay_is_random and self.delay_uniform < 3:
            return False
        return not self.delay_is_random or self.delay_random_min >= 3

    def to_dict(self):
        return {
            "URL_SCRAPE_DELAY_IS_REQUIRED": self.delay_is_required,
            "URL_SCRAPE_DELAY_IS_RANDOM": self.delay_is_random,
            "URL_SCRAPE_DELAY_IN_SECONDS": self.delay_uniform,
            "URL_SCRAPE_DELAY_IN_SECONDS_MIN": self.delay_random_min,
            "URL_SCRAPE_DELAY_IN_SECONDS_MAX": self.delay_random_max,
        }

    @staticmethod
    def from_config(config_dict):
        delay_is_required = config_dict.get("URL_SCRAPE_DELAY_IS_REQUIRED", True)
        delay_is_random = config_dict.get("URL_SCRAPE_DELAY_IS_RANDOM", True)
        delay_uniform = config_dict.get("URL_SCRAPE_DELAY_IN_SECONDS", 0)
        delay_random_min = config_dict.get("URL_SCRAPE_DELAY_IN_SECONDS_MIN", 3)
        delay_random_max = config_dict.get("URL_SCRAPE_DELAY_IN_SECONDS_MAX", 6)
        return UrlScrapeDelay(
            delay_is_required=delay_is_required,
            delay_is_random=delay_is_random,
            delay_uniform=delay_uniform,
            delay_random_min=delay_random_min,
            delay_random_max=delay_random_max,
        )

    @staticmethod
    def null_object():
        return {
            "URL_SCRAPE_DELAY_IS_REQUIRED": None,
            "URL_SCRAPE_DELAY_IS_RANDOM": None,
            "URL_SCRAPE_DELAY_IN_SECONDS": None,
            "URL_SCRAPE_DELAY_IN_SECONDS_MIN": None,
            "URL_SCRAPE_DELAY_IN_SECONDS_MAX": None,
        }
