import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Mapping

from dateutil import tz

from vigorish.scrape.brooks_pitchfx.models.pitchfx import BrooksPitchFxData
from vigorish.util.list_helpers import as_dict_list


@dataclass
class BrooksPitchFxLog:
    pitch_count_by_inning: Mapping
    total_pitch_count: str = "0"
    pitcher_name: str = ""
    pitcher_id_mlb: str = "0"
    pitch_app_id: str = ""
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
    pitchfx_log: List[BrooksPitchFxData] = field(default_factory=list)

    @property
    def game_date(self):
        return datetime(self.game_date_year, self.game_date_month, self.game_date_day).date()

    @property
    def game_start_time(self):
        if not self.game_time_hour:
            return None
        game_start_time = datetime(
            year=self.game_date.year,
            month=self.game_date.month,
            day=self.game_date.day,
            hour=self.game_time_hour,
            minute=self.game_time_minute,
        )
        return game_start_time.replace(tzinfo=tz.gettz(self.time_zone_name))

    def as_dict(self):
        return {
            "__brooks_pitchfx_log__": True,
            "pitchfx_log": as_dict_list(self.pitchfx_log),
            "pitch_count_by_inning": self.pitch_count_by_inning,
            "pitcher_name": self.pitcher_name,
            "pitcher_id_mlb": int(self.pitcher_id_mlb),
            "pitch_app_id": self.pitch_app_id,
            "total_pitch_count": int(self.total_pitch_count),
            "pitcher_team_id_bb": self.pitcher_team_id_bb,
            "opponent_team_id_bb": self.opponent_team_id_bb,
            "bb_game_id": self.bb_game_id,
            "bbref_game_id": self.bbref_game_id,
            "game_date_year": int(self.game_date_year),
            "game_date_month": int(self.game_date_month),
            "game_date_day": int(self.game_date_day),
            "game_time_hour": int(self.game_time_hour),
            "game_time_minute": int(self.game_time_minute),
            "time_zone_name": self.time_zone_name,
            "pitchfx_url": self.pitchfx_url,
        }

    def as_json(self):
        """Convert pitchfx log to JSON."""
        return json.dumps(self.as_dict(), indent=2)
