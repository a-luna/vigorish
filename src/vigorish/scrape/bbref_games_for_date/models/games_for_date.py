"""Boxscore URLs for all games that took place on a single day."""
import json


class BBRefGamesForDate:
    """Boxscore URLs for all games that took place on a single day."""

    dashboard_url = ""
    game_date = None
    game_date_str = ""
    game_count = ""
    boxscore_urls = []

    def as_dict(self):
        """Convert daily boxscore URL values to a dictionary."""
        return dict(
            __bbref_games_for_date__=True,
            dashboard_url=self.dashboard_url,
            game_date_str=self.game_date_str,
            game_count=int(self.game_count),
            boxscore_urls=self.boxscore_urls,
        )

    def as_json(self):
        """Convert daily game list to JSON."""
        return json.dumps(self.as_dict(), indent=2, sort_keys=True)
