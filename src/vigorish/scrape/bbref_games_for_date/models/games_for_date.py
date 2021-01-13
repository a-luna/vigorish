"""Boxscore URLs for all games that took place on a single day."""
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import List

from vigorish.scrape.bbref_games_for_date.models.game_info import BBRefGameInfo
from vigorish.util.list_helpers import as_dict_list


@dataclass
class BBRefGamesForDate:
    """BBRef game ids and boxscore URLs for all games that took place on a single day."""

    dashboard_url: str = ""
    game_date: datetime = None
    game_date_str: str = ""
    game_count: str = "0"
    games: List[BBRefGameInfo] = field(default_factory=list)

    @property
    def all_urls(self):
        return [game.url for game in self.games]

    @property
    def all_bbref_game_ids(self):
        return [game.bbref_game_id for game in self.games]

    def as_dict(self):
        """Convert bbref games for date to a dictionary."""
        return {
            "__bbref_games_for_date__": True,
            "dashboard_url": self.dashboard_url,
            "game_date_str": self.game_date_str,
            "game_count": int(self.game_count),
            "games": as_dict_list(self.games),
        }

    def as_json(self):
        """Convert daily game list to JSON."""
        return json.dumps(self.as_dict(), indent=2, sort_keys=True)
