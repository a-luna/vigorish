"""Boxscore URLs for all games that took place on a single day."""
import json

class BbrefGamesForDate():
    """Boxscore URLs for all games that took place on a single day."""

    scrape_success = ""
    scrape_error = ""
    url = ""
    game_date = ""
    game_count = ""
    boxscore_urls = []

    def as_dict(self):
        """Convert daily boxscore URL values to a dictionary."""
        dict = {
            "scrape_success": "{}".format(self.scrape_success),
            "scrape_error": "{}".format(self.scrape_error),
            "url": "{}".format(self.url),
            "game_date": "{}".format(self.game_date),
            "game_count": int(self.game_count),
            "boxscore_urls": self.boxscore_urls
        }
        return dict
