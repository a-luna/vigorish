"""A single pitching appearance scraped from brooksbaseball.com."""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict

from dateutil import tz


@dataclass
class BrooksPitchLog:
    """A single pitching appearance scraped from brooksbaseball.com."""

    parsed_all_info: bool = False
    pitcher_name: str = ""
    pitcher_id_mlb: int = 0
    pitch_app_id: str = ""
    total_pitch_count: int = 0
    pitch_count_by_inning: Dict[str, int] = field(default_factory=dict)
    pitcher_team_id_bb: str = ""
    opponent_team_id_bb: str = ""
    bb_game_id: str = ""
    bbref_game_id: str = ""
    game_date_year: str = ""
    game_date_month: str = ""
    game_date_day: str = ""
    game_time_hour: str = ""
    game_time_minute: str = ""
    time_zone_name: str = ""
    pitchfx_url: str = ""
    pitch_log_url: str = ""

    @property
    def game_date(self):
        return datetime(self.game_date_year, self.game_date_month, self.game_date_day).date()

    @property
    def game_start_time(self):
        game_start = datetime(
            year=self.game_date.year,
            month=self.game_date.month,
            day=self.game_date.day,
            hour=self.game_time_hour,
            minute=self.game_time_minute,
        )
        return (
            game_start.replace(tzinfo=tz.gettz(self.time_zone_name))
            if self.game_time_hour != 0 or self.game_time_minute != 0
            else None
        )

    def as_dict(self):
        """Convert pitch log to a dictionary."""
        return {
            "__brooks_pitch_log__": True,
            "parsed_all_info": self.parsed_all_info,
            "pitcher_name": f"{self.pitcher_name}",
            "pitcher_id_mlb": self.pitcher_id_mlb,
            "pitch_app_id": f"{self.pitch_app_id}",
            "total_pitch_count": self.total_pitch_count,
            "pitch_count_by_inning": self.pitch_count_by_inning,
            "pitcher_team_id_bb": f"{self.pitcher_team_id_bb}",
            "opponent_team_id_bb": f"{self.opponent_team_id_bb}",
            "bb_game_id": f"{self.bb_game_id}",
            "bbref_game_id": f"{self.bbref_game_id}",
            "game_date_year": int(self.game_date_year),
            "game_date_month": int(self.game_date_month),
            "game_date_day": int(self.game_date_day),
            "game_time_hour": int(self.game_time_hour),
            "game_time_minute": int(self.game_time_minute),
            "time_zone_name": self.time_zone_name,
            "pitchfx_url": f"{self.pitchfx_url}",
            "pitch_log_url": f"{self.pitch_log_url}",
        }
