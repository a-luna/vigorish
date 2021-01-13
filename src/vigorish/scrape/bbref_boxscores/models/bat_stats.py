from dataclasses import dataclass, field
from typing import List

from vigorish.scrape.bbref_boxscores.models.bat_stats_detail import BBRefBatStatsDetail
from vigorish.util.list_helpers import as_dict_list


@dataclass
class BBRefBatStats:
    """Statistics for all plate appearances that occurred for one player in a single game."""

    player_id_br: str = ""
    player_team_id_br: str = ""
    opponent_team_id_br: str = ""
    plate_appearances: str = "0"
    at_bats: str = "0"
    hits: str = "0"
    runs_scored: str = "0"
    rbis: str = "0"
    bases_on_balls: str = "0"
    strikeouts: str = "0"
    avg_to_date: str = "0"
    obp_to_date: str = "0"
    slg_to_date: str = "0"
    ops_to_date: str = "0"
    total_pitches: str = "0"
    total_strikes: str = "0"
    wpa_bat: str = "0"
    avg_lvg_index: str = "0"
    wpa_bat_pos: str = "0"
    wpa_bat_neg: str = "0"
    re24_bat: str = "0"
    details: List[BBRefBatStatsDetail] = field(default_factory=list)

    def as_dict(self):
        """Convert batting statistics for one player in a single game to a dictionary."""
        return {
            "player_id_br": self.player_id_br,
            "player_team_id_br": self.player_team_id_br,
            "opponent_team_id_br": self.opponent_team_id_br,
            "plate_appearances": int(self.plate_appearances),
            "at_bats": int(self.at_bats),
            "hits": int(self.hits),
            "runs_scored": int(self.runs_scored),
            "rbis": int(self.rbis),
            "bases_on_balls": int(self.bases_on_balls),
            "strikeouts": int(self.strikeouts),
            "avg_to_date": float(self.avg_to_date),
            "obp_to_date": float(self.obp_to_date),
            "slg_to_date": float(self.slg_to_date),
            "ops_to_date": float(self.ops_to_date),
            "total_pitches": int(self.total_pitches),
            "total_strikes": int(self.total_strikes),
            "wpa_bat": float(self.wpa_bat),
            "avg_lvg_index": float(self.avg_lvg_index),
            "wpa_bat_pos": float(self.wpa_bat_pos),
            "wpa_bat_neg": float(self.wpa_bat_neg),
            "re24_bat": float(self.re24_bat),
            "details": as_dict_list(self.details),
        }
