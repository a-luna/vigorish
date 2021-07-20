"""Individual game info scraped from brooksbaseball.com."""
from dataclasses import dataclass, field
from typing import Dict

from vigorish.util.string_helpers import validate_bbref_game_id


@dataclass(eq=True)
class BrooksGameInfo:
    """Individual game info scraped from brooksbaseball.com."""

    might_be_postponed: bool = field(repr=False, default=None)
    game_date_year: str = field(repr=False, default="")
    game_date_month: str = field(repr=False, default="")
    game_date_day: str = field(repr=False, default="")
    game_time_hour: str = field(repr=False, default="")
    game_time_minute: str = field(repr=False, default="")
    time_zone_name: str = field(repr=False, default="")
    mlb_game_id: str = field(repr=False, default="")
    bb_game_id: str = field(repr=False, default="")
    bbref_game_id: str = ""
    away_team_id_bb: str = field(repr=False, default="")
    home_team_id_bb: str = field(repr=False, default="")
    game_number_this_day: int = field(repr=False, default=0)
    pitcher_appearance_count: int = field(repr=False, default=0)
    pitcher_appearance_dict: Dict[str, str] = field(repr=False, default_factory=dict)

    @property
    def game_date(self):
        game_dict = validate_bbref_game_id(self.bbref_game_id).value
        return game_dict["game_date"]

    @property
    def all_pitch_app_ids(self):
        return [f"{self.bbref_game_id}_{pitch_app_id}" for pitch_app_id in self.pitcher_appearance_dict.keys()]

    def as_dict(self):
        """Convert game info list to a dictionary."""
        return {
            "__brooks_game_info__": True,
            "might_be_postponed": self.might_be_postponed,
            "game_date_year": int(self.game_date_year),
            "game_date_month": int(self.game_date_month),
            "game_date_day": int(self.game_date_day),
            "game_time_hour": int(self.game_time_hour),
            "game_time_minute": int(self.game_time_minute),
            "time_zone_name": self.time_zone_name,
            "bb_game_id": self.bb_game_id,
            "bbref_game_id": self.bbref_game_id,
            "away_team_id_bb": self.away_team_id_bb,
            "home_team_id_bb": self.home_team_id_bb,
            "game_number_this_day": self.game_number_this_day,
            "pitcher_appearance_count": int(self.pitcher_appearance_count),
            "pitcher_appearance_dict": self.pitcher_appearance_dict,
        }
