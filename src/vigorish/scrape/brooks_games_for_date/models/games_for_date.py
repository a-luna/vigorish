"""Boxscore URLs for all games that took place on a single day."""
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import List

from vigorish.scrape.brooks_games_for_date.models.game_info import BrooksGameInfo
from vigorish.util.list_helpers import as_dict_list, flatten_list2d


@dataclass
class BrooksGamesForDate:
    """Boxscore URLs for all games that took place on a single day."""

    dashboard_url: str = ""
    game_date: datetime = None
    game_date_str: str = ""
    game_count: str = "0"
    games: List[BrooksGameInfo] = field(default_factory=list)

    @property
    def all_pitch_app_ids_for_date(self):
        return sorted(flatten_list2d([g.all_pitch_app_ids for g in self.games]))

    @property
    def all_bbref_game_ids(self):
        return [game.bbref_game_id for game in self.games]

    def as_dict(self):
        """Convert daily boxscore URL values to a dictionary."""
        return {
            "__brooks_games_for_date__": True,
            "dashboard_url": self.dashboard_url,
            "game_date_str": self.game_date_str,
            "game_count": int(self.game_count),
            "games": as_dict_list(self.games),
        }

    def as_json(self):
        """Convert daily game list to JSON."""
        return json.dumps(self.as_dict(), indent=2)
