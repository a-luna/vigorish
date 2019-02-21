"""Boxscore URLs for all games that took place on a single day."""
import json

class BrooksGamesForDate():
    """Boxscore URLs for all games that took place on a single day."""

    dashboard_url = ""
    game_date = None
    game_date_str = ""
    game_count = ""
    games = []

    def as_dict(self):
        """Convert daily boxscore URL values to a dictionary."""
        dict = {
            "__brooks_games_for_date__": True,
            "dashboard_url": "{}".format(self.dashboard_url),
            "game_date_str": "{}".format(self.game_date_str),
            "game_count": int(self.game_count),
            "games": self._flatten(self.games)
        }
        return dict

    def as_json(self):
        """Convert daily game list to JSON."""
        return json.dumps(self.as_dict(), indent=2, sort_keys=True)

    @staticmethod
    def _flatten(objects):
        return [obj.as_dict() for obj in objects]

