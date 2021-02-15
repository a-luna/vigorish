from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Union

from dacite import from_dict
from sqlalchemy.engine import RowProxy

from vigorish.data.metrics.constants import (
    PITCH_STATS_FLOAT_METRIC_NAMES,
    PITCH_STATS_INT_METRIC_NAMES,
    PITCH_STATS_STR_METRIC_NAMES,
)
from vigorish.types import RowDict


@dataclass
class PitchStatsMetrics:
    mlb_id: int
    year: int = field(repr=False, default=0)
    player_team_id_bbref: str = field(repr=False, default="")
    opponent_team_id_bbref: str = field(repr=False, default="")
    stint_number: int = field(repr=False, default=0)
    total_games: int = field(repr=False, default=0)
    games_as_sp: int = field(repr=False, default=0)
    games_as_rp: int = field(repr=False, default=0)
    wins: int = field(repr=False, default=0)
    losses: int = field(repr=False, default=0)
    saves: int = field(repr=False, default=0)
    innings_pitched: float = 0.0
    total_outs: int = field(repr=False, default=0)
    batters_faced: int = field(repr=False, default=0)
    runs: int = field(repr=False, default=0)
    earned_runs: int = field(repr=False, default=0)
    hits: int = field(repr=False, default=0)
    homeruns: int = field(repr=False, default=0)
    strikeouts: int = field(repr=False, default=0)
    bases_on_balls: int = field(repr=False, default=0)
    era: float = 0.0
    whip: float = 0.0
    k_per_nine: float = field(repr=False, default=0.0)
    bb_per_nine: float = field(repr=False, default=0.0)
    hr_per_nine: float = field(repr=False, default=0.0)
    k_per_bb: float = field(repr=False, default=0.0)
    k_rate: float = field(repr=False, default=0.0)
    bb_rate: float = field(repr=False, default=0.0)
    k_minus_bb: float = field(repr=False, default=0.0)
    hr_per_fb: float = field(repr=False, default=0.0)
    pitch_count: int = field(repr=False, default=0)
    strikes: int = field(repr=False, default=0)
    strikes_contact: int = field(repr=False, default=0)
    strikes_swinging: int = field(repr=False, default=0)
    strikes_looking: int = field(repr=False, default=0)
    ground_balls: int = field(repr=False, default=0)
    fly_balls: int = field(repr=False, default=0)
    line_drives: int = field(repr=False, default=0)
    unknown_type: int = field(repr=False, default=0)
    inherited_runners: int = field(repr=False, default=0)
    inherited_scored: int = field(repr=False, default=0)
    wpa_pitch: float = field(repr=False, default=0.0)
    re24_pitch: float = field(repr=False, default=0.0)

    @classmethod
    def from_pitch_stats_view_results(cls, results: List[RowProxy]) -> PitchStatsMetrics:
        row_dicts = [dict(row) for row in results]
        if not row_dicts:
            return None
        mlb_id = row_dicts[0]["mlb_id"]
        pitch_stats_list = [_get_pitch_stats_for_table(mlb_id, d) for d in row_dicts]
        return [from_dict(data_class=cls, data=pitch_stats) for pitch_stats in pitch_stats_list]


PitchStatsMetricsDict = Dict[str, Union[int, float]]


def _get_pitch_stats_for_table(mlb_id: int, pitch_stats_dict: RowDict) -> PitchStatsMetricsDict:
    pitch_stats = {"mlb_id": mlb_id}
    for metric in PITCH_STATS_FLOAT_METRIC_NAMES:
        if metric in pitch_stats_dict:
            pitch_stats[metric] = round(pitch_stats_dict[metric], 3)
    for metric in PITCH_STATS_INT_METRIC_NAMES:
        if metric in pitch_stats_dict:
            pitch_stats[metric] = pitch_stats_dict[metric]
    for metric in PITCH_STATS_STR_METRIC_NAMES:
        if metric in pitch_stats_dict:
            pitch_stats[metric] = pitch_stats_dict[metric]
    return pitch_stats
