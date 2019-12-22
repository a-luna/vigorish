from dataclasses import dataclass
from typing import Any

from app.main.constants import TEAM_ID_DICT
from app.main.models.player import Player


@dataclass
class BBRefPlayByPlayEvent():
    """Various numeric and string values that describe a plate appearance."""

    event_id: str = "0"
    inning_id: str = "0"
    inning_label: str = "0"
    pbp_table_row_number: str = "0"
    score: str = "0"
    outs_before_play: str = "0"
    runners_on_base: str = "0"
    pitch_sequence: str = "0"
    runs_outs_result: str = "0"
    team_batting_id_br: str = "0"
    team_pitching_id_br: str = "0"
    play_description: str = "0"
    pitcher_id_br: str = "0"
    batter_id_br: str = "0"
    play_index_url: str = "0"


    @property
    def at_bat_id(self):
        bbref_game_id = self.event_id.split("-")[0]
        inning_num = self.inning_label[1:]
        team_pitching_id_bb = self._get_brooks_team_id(self.team_pitching_id_br).lower()
        pitcher_id_mlb = Player.find_by_bbref_id(self.pitcher_id_br).mlb_id
        team_batting_id_bb = self._get_brooks_team_id(self.team_batting_id_br).lower()
        batter_id_mlb = Player.find_by_bbref_id(self.batter_id_br).mlb_id
        return f"{bbref_game_id}_{inning_num}_{team_pitching_id_bb}_{pitcher_id_mlb}_{team_batting_id_bb}_{batter_id_mlb}"

    def as_dict(self):
        """Convert game event values to a dictionary."""
        dict = {
            "__bbref_pbp_game_event__": True,
            "event_id": self.event_id,
            "inning_id": self.inning_id,
            "inning_label": self.inning_label,
            'pbp_table_row_number': int(self.pbp_table_row_number),
            "score": self.score,
            "outs_before_play": int(self.outs_before_play),
            "runners_on_base": self.runners_on_base,
            "pitch_sequence": self.pitch_sequence,
            "runs_outs_result": self.runs_outs_result,
            "team_batting_id_br": self.team_batting_id_br,
            "team_pitching_id_br": self.team_pitching_id_br,
            "play_description": self.play_description,
            "pitcher_id_br": self.pitcher_id_br,
            "batter_id_br": self.batter_id_br,
            "play_index_url": self.play_index_url
        }
        return dict

    @staticmethod
    def _get_brooks_team_id(br_team_id):
        if br_team_id in TEAM_ID_DICT:
            return TEAM_ID_DICT[br_team_id]
        return br_team_id
