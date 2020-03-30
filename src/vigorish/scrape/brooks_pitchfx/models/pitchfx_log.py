import json
from dataclasses import dataclass
from typing import Any

from vigorish.util.string_helpers import validate_brooks_game_id
from vigorish.util.list_helpers import as_dict_list


@dataclass
class BrooksPitchFxLog:
    pitchfx_log: Any
    pitch_count_by_inning: Any
    total_pitch_count: str = "0"
    pitcher_name: str = ""
    pitcher_id_mlb: str = "0"
    pitch_app_id: str = ""
    pitcher_team_id_bb: str = ""
    opponent_team_id_bb: str = ""
    bb_game_id: str = ""
    bbref_game_id: str = ""
    pitchfx_url: str = ""

    @property
    def game_date(self):
        result = validate_brooks_game_id(self.bb_game_id)
        if result.failure:
            return None
        game_dict = result.value
        return game_dict["game_date"]

    def as_dict(self):
        return dict(
            __brooks_pitchfx_log__=True,
            pitchfx_log=as_dict_list(self.pitchfx_log),
            pitch_count_by_inning=self.pitch_count_by_inning,
            pitcher_name=self.pitcher_name,
            pitcher_id_mlb=int(self.pitcher_id_mlb),
            pitch_app_id=self.pitch_app_id,
            total_pitch_count=int(self.total_pitch_count),
            pitcher_team_id_bb=self.pitcher_team_id_bb,
            opponent_team_id_bb=self.opponent_team_id_bb,
            bb_game_id=self.bb_game_id,
            bbref_game_id=self.bbref_game_id,
            pitchfx_url=self.pitchfx_url,
        )

    def as_json(self):
        """Convert pitchfx log to JSON."""
        return json.dumps(self.as_dict(), indent=2)
