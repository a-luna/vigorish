"""Boxscore URLs for all games that took place on a single day."""
import json
from collections import ChainMap

from vigorish.util.list_helpers import as_dict_list, flatten_list2d


class BrooksGamesForDate:
    """Boxscore URLs for all games that took place on a single day."""

    dashboard_url = ""
    game_date = None
    game_date_str = ""
    game_count = ""
    games = []

    @property
    def game_id_dict(self):
        dict_list = [g.game_id_dict for g in self.games if not g.might_be_postponed]
        return dict(ChainMap(*dict_list))

    @property
    def all_pitch_app_ids_for_date(self):
        return sorted(flatten_list2d([g.all_pitch_app_ids for g in self.games]))

    def as_dict(self):
        """Convert daily boxscore URL values to a dictionary."""
        return dict(
            __brooks_games_for_date__=True,
            dashboard_url=self.dashboard_url,
            game_date_str=self.game_date_str,
            game_count=int(self.game_count),
            games=as_dict_list(self.games),
        )

    def as_json(self):
        """Convert daily game list to JSON."""
        return json.dumps(self.as_dict(), indent=2)

    def retrieve_game_info(self, bbref_game_id):
        game_match = [game for game in self.games if game.bbref_game_id == bbref_game_id]
        return game_match[0] if game_match else None
