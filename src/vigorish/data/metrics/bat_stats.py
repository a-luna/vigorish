from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from dacite import from_dict
from sqlalchemy.engine import RowProxy

from vigorish.enums import DefensePosition
from vigorish.types import MetricsDict, RowDict
from vigorish.util.dataclass_helpers import get_field_types


@dataclass
class BatStatsMetrics:
    year: int = field(repr=False, default=0)
    team_id_bbref: str = field(repr=False, default="")
    opponent_team_id_bbref: str = field(repr=False, default="")
    is_starter: bool = field(repr=False, default=False)
    bat_order: int = field(repr=False, default=0)
    def_position: DefensePosition = field(repr=False, default=DefensePosition.NONE)
    mlb_id: int = field(repr=False, default=0)
    bbref_id: str = field(repr=False, default="")
    stint_number: int = field(repr=False, default=0)
    total_games: int = field(repr=False, default=0)
    avg: float = 0.0
    obp: float = field(repr=False, default=0.0)
    slg: float = field(repr=False, default=0.0)
    ops: float = 0.0
    iso: float = field(repr=False, default=0.0)
    bb_rate: float = field(repr=False, default=0.0)
    k_rate: float = field(repr=False, default=0.0)
    contact_rate: float = field(repr=False, default=0.0)
    plate_appearances: int = 0
    at_bats: int = field(repr=False, default=0)
    hits: int = field(repr=False, default=0)
    runs_scored: int = field(repr=False, default=0)
    rbis: int = field(repr=False, default=0)
    bases_on_balls: int = field(repr=False, default=0)
    strikeouts: int = field(repr=False, default=0)
    doubles: int = field(repr=False, default=0)
    triples: int = field(repr=False, default=0)
    homeruns: int = field(repr=False, default=0)
    stolen_bases: int = field(repr=False, default=0)
    caught_stealing: int = field(repr=False, default=0)
    hit_by_pitch: int = field(repr=False, default=0)
    intentional_bb: int = field(repr=False, default=0)
    gdp: int = field(repr=False, default=0)
    sac_fly: int = field(repr=False, default=0)
    sac_hit: int = field(repr=False, default=0)
    total_pitches: int = field(repr=False, default=0)
    total_strikes: int = field(repr=False, default=0)
    wpa_bat: float = field(repr=False, default=0.0)
    wpa_bat_pos: float = field(repr=False, default=0.0)
    wpa_bat_neg: float = field(repr=False, default=0.0)
    re24_bat: float = field(repr=False, default=0.0)

    @classmethod
    def from_query_results(cls, results: List[RowProxy]) -> List[BatStatsMetrics]:
        return [from_dict(data_class=cls, data=cls._get_bat_stats(dict(row))) for row in results]

    @classmethod
    def _get_bat_stats(cls, bat_stats: RowDict, decimal_precision=3) -> MetricsDict:
        dc_fields = get_field_types(cls)
        return {
            k: round(v, decimal_precision)
            if dc_fields[k] is float
            else bool(v)
            if dc_fields[k] is bool
            else DefensePosition(int(v))
            if dc_fields[k] is DefensePosition
            else v
            for k, v in bat_stats.items()
            if v and k in dc_fields
        }
