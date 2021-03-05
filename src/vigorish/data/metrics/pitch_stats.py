from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from dacite import from_dict
from sqlalchemy.engine import RowProxy

from vigorish.types import MetricsDict, RowDict
from vigorish.util.dataclass_helpers import get_field_types


@dataclass
class PitchStatsMetrics:
    year: int = field(repr=False, default=0)
    team_id_bbref: str = field(repr=False, default="")
    opponent_team_id_bbref: str = field(repr=False, default="")
    mlb_id: int = field(repr=False, default=0)
    bbref_id: str = field(repr=False, default="")
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
    def from_query_results(cls, results: List[RowProxy]) -> List[PitchStatsMetrics]:
        return [from_dict(data_class=cls, data=cls._get_pitch_stats(dict(row))) for row in results]

    @classmethod
    def _get_pitch_stats(cls, pitch_stats: RowDict, decimal_precision: int = 3) -> MetricsDict:
        dc_fields = get_field_types(cls)
        return {
            k: round(v, decimal_precision) if dc_fields[k] is float else v
            for k, v in pitch_stats.items()
            if v and k in dc_fields
        }
