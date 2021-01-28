from dataclasses import dataclass


@dataclass
class BatchJobSettings:
    batched_scraping_enabled: bool
    batch_size_is_random: bool
    batch_size_uniform: int
    batch_size_random_min: int
    batch_size_random_max: int

    def __str__(self):
        if not self.batched_scraping_enabled:
            return "Batched scraping is not enabled"
        if not self.batch_size_is_random:
            return f"Batch size is uniform ({self.batch_size_uniform} URLs)"
        return f"Batch size is random ({self.batch_size_random_min}-{self.batch_size_random_max} URLs)"

    def to_dict(self):
        return {
            "CREATE_BATCHED_SCRAPE_JOBS": self.batched_scraping_enabled,
            "USE_IRREGULAR_BATCH_SIZES": self.batch_size_is_random,
            "BATCH_SIZE": self.batch_size_uniform,
            "BATCH_SIZE_MIN": self.batch_size_random_min,
            "BATCH_SIZE_MAX": self.batch_size_random_max,
        }

    @staticmethod
    def from_config(config_dict):
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
    def null_object():
        return {
            "CREATE_BATCHED_SCRAPE_JOBS": None,
            "USE_IRREGULAR_BATCH_SIZES": None,
            "BATCH_SIZE": None,
            "BATCH_SIZE_MIN": None,
            "BATCH_SIZE_MAX": None,
        }
