from dataclasses import dataclass


@dataclass
class BatchScrapeDelay:
    delay_is_required: bool
    delay_is_random: bool
    delay_uniform: int
    delay_random_min: int
    delay_random_max: int

    def __str__(self):
        if not self.delay_is_required:
            return "No delay after each batch"
        if not self.delay_is_random:
            return f"Delay is uniform ({self.delay_uniform} minutes)"
        return f"Delay is random ({self.delay_random_min}-{self.delay_random_max} minutes)"

    @property
    def delay_uniform_ms(self):
        return self.delay_uniform * 60 * 1000 if self.delay_uniform else 0

    @property
    def delay_random_min_ms(self):
        return self.delay_random_min * 60 * 1000 if self.delay_random_min else 0

    @property
    def delay_random_max_ms(self):
        return self.delay_random_max * 60 * 1000 if self.delay_random_max else 0

    def to_dict(self):
        return {
            "BATCH_SCRAPE_DELAY_IS_REQUIRED": self.delay_is_required,
            "BATCH_SCRAPE_DELAY_IS_RANDOM": self.delay_is_random,
            "BATCH_SCRAPE_DELAY_IN_MINUTES": self.delay_uniform,
            "BATCH_SCRAPE_DELAY_IN_MINUTES_MIN": self.delay_random_min,
            "BATCH_SCRAPE_DELAY_IN_MINUTES_MAX": self.delay_random_max,
        }

    @staticmethod
    def from_config(config_dict):
        delay_is_required = config_dict.get("BATCH_SCRAPE_DELAY_IS_REQUIRED", True)
        delay_is_random = config_dict.get("BATCH_SCRAPE_DELAY_IS_RANDOM", True)
        delay_uniform = config_dict.get("BATCH_SCRAPE_DELAY_IN_MINUTES", 0)
        delay_random_min = config_dict.get("BATCH_SCRAPE_DELAY_IN_MINUTES_MIN", 5)
        delay_random_max = config_dict.get("BATCH_SCRAPE_DELAY_IN_MINUTES_MAX", 10)
        return BatchScrapeDelay(
            delay_is_required=delay_is_required,
            delay_is_random=delay_is_random,
            delay_uniform=delay_uniform,
            delay_random_min=delay_random_min,
            delay_random_max=delay_random_max,
        )

    @staticmethod
    def null_object():
        return {
            "BATCH_SCRAPE_DELAY_IS_REQUIRED": None,
            "BATCH_SCRAPE_DELAY_IS_RANDOM": None,
            "BATCH_SCRAPE_DELAY_IN_MINUTES": None,
            "BATCH_SCRAPE_DELAY_IN_MINUTES_MIN": None,
            "BATCH_SCRAPE_DELAY_IN_MINUTES_MAX": None,
        }
