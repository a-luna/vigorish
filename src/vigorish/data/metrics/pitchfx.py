from __future__ import annotations

from dataclasses import dataclass, field
from functools import cached_property
from typing import Dict, List, Union

from dacite import from_dict
from sqlalchemy.engine import RowProxy

from vigorish.data.metrics.constants import PFX_BATTED_BALL_METRICS, PFX_PLATE_DISCIPLINE_METRICS
from vigorish.enums import PitchType
from vigorish.types import RowDict
from vigorish.util.dataclass_helpers import get_field_types
from vigorish.util.string_helpers import format_decimal_bat_stat

PitchFxMetricsDict = Dict[str, Union[bool, int, float, str, PitchType]]
PitchTypeMetricsDict = Dict[PitchType, PitchFxMetricsDict]


@dataclass
class PitchFxMetrics:
    mlb_id: int = field(repr=False, default=0)
    p_throws: str = field(repr=False, default="")
    bat_stand: str = field(repr=False, default="")
    pitch_type: PitchType = PitchType.NONE
    total_pitches: int = field(repr=False, default=0)
    total_pa: int = field(repr=False, default=0)
    total_at_bats: int = field(repr=False, default=0)
    total_outs: int = field(repr=False, default=0)
    total_hits: int = field(repr=False, default=0)
    total_bb: int = field(repr=False, default=0)
    total_k: int = field(repr=False, default=0)
    avg_speed: float = 0.0
    avg: float = field(repr=False, default=0.0)
    obp: float = field(repr=False, default=0.0)
    slg: float = field(repr=False, default=0.0)
    ops: float = field(repr=False, default=0.0)
    iso: float = field(repr=False, default=0.0)
    fly_ball_rate: float = field(repr=False, default=0.0)
    ground_ball_rate: float = field(repr=False, default=0.0)
    line_drive_rate: float = field(repr=False, default=0.0)
    popup_rate: float = field(repr=False, default=0.0)
    hard_hit_rate: float = field(repr=False, default=0.0)
    medium_hit_rate: float = field(repr=False, default=0.0)
    soft_hit_rate: float = field(repr=False, default=0.0)
    barrel_rate: float = field(repr=False, default=0.0)
    avg_launch_speed: float = field(repr=False, default=0.0)
    max_launch_speed: float = field(repr=False, default=0.0)
    avg_launch_angle: float = field(repr=False, default=0.0)
    avg_hit_distance: float = field(repr=False, default=0.0)
    bb_rate: float = field(repr=False, default=0.0)
    k_rate: float = field(repr=False, default=0.0)
    hr_per_fb: float = field(repr=False, default=0.0)
    avg_pfx_x: float = field(repr=False, default=0.0)
    avg_pfx_z: float = field(repr=False, default=0.0)
    avg_px: float = field(repr=False, default=0.0)
    avg_pz: float = field(repr=False, default=0.0)
    avg_plate_time: float = field(repr=False, default=0.0)
    avg_extension: float = field(repr=False, default=0.0)
    avg_break_angle: float = field(repr=False, default=0.0)
    avg_break_length: float = field(repr=False, default=0.0)
    avg_break_y: float = field(repr=False, default=0.0)
    avg_spin_rate: float = field(repr=False, default=0.0)
    avg_spin_direction: float = field(repr=False, default=0.0)
    zone_rate: float = field(repr=False, default=0.0)
    called_strike_rate: float = field(repr=False, default=0.0)
    swinging_strike_rate: float = field(repr=False, default=0.0)
    whiff_rate: float = field(repr=False, default=0.0)
    csw_rate: float = field(repr=False, default=0.0)
    o_swing_rate: float = field(repr=False, default=0.0)
    z_swing_rate: float = field(repr=False, default=0.0)
    swing_rate: float = field(repr=False, default=0.0)
    o_contact_rate: float = field(repr=False, default=0.0)
    z_contact_rate: float = field(repr=False, default=0.0)
    contact_rate: float = field(repr=False, default=0.0)
    total_swings: int = field(repr=False, default=0)
    total_swings_made_contact: int = field(repr=False, default=0)
    total_called_strikes: int = field(repr=False, default=0)
    total_swinging_strikes: int = field(repr=False, default=0)
    total_inside_strike_zone: int = field(repr=False, default=0)
    total_outside_strike_zone: int = field(repr=False, default=0)
    total_swings_inside_zone: int = field(repr=False, default=0)
    total_swings_outside_zone: int = field(repr=False, default=0)
    total_contact_inside_zone: int = field(repr=False, default=0)
    total_contact_outside_zone: int = field(repr=False, default=0)
    total_balls_in_play: int = field(repr=False, default=0)
    total_ground_balls: int = field(repr=False, default=0)
    total_line_drives: int = field(repr=False, default=0)
    total_fly_balls: int = field(repr=False, default=0)
    total_popups: int = field(repr=False, default=0)
    total_hard_hits: int = field(repr=False, default=0)
    total_medium_hits: int = field(repr=False, default=0)
    total_soft_hits: int = field(repr=False, default=0)
    total_barrels: int = field(repr=False, default=0)
    total_singles: int = field(repr=False, default=0)
    total_doubles: int = field(repr=False, default=0)
    total_triples: int = field(repr=False, default=0)
    total_homeruns: int = field(repr=False, default=0)
    total_ibb: int = field(repr=False, default=0)
    total_hbp: int = field(repr=False, default=0)
    total_errors: int = field(repr=False, default=0)
    total_sac_hit: int = field(repr=False, default=0)
    total_sac_fly: int = field(repr=False, default=0)
    pitch_type_int: int = field(repr=False, default=0)
    percent: float = 0.0

    @property
    def pitch_name(self) -> str:
        return self.pitch_type.print_name

    @property
    def triple_slash(self) -> str:
        return (
            f"{format_decimal_bat_stat(self.avg)}/"
            f"{format_decimal_bat_stat(self.obp)}/"
            f"{format_decimal_bat_stat(self.slg)} ({format_decimal_bat_stat(self.ops)})"
        )

    def get_bat_stats(self, include_pa_count: bool = False) -> Dict[str, str]:
        total_pa = f" ({self.total_pa})" if include_pa_count else ""
        return {
            "pitch_type": f"{self.pitch_name}{total_pa}",
            "AVG/OBP/SLG (OPS)": self.triple_slash,
            "K%": f"{self.k_rate:.0%}",
            "BB%": f"{self.bb_rate:.0%}",
            "HR/FB": f"{self.hr_per_fb:.0%}",
        }

    def get_plate_discipline_stats(self, include_pitch_count: bool = False) -> Dict[str, str]:
        total_pitches = f" ({self.total_pitches})" if include_pitch_count else ""
        pd_stats = {"pitch_type": f"{self.pitch_name}{total_pitches}"}
        for metric, (rate, total) in PFX_PLATE_DISCIPLINE_METRICS.items():
            pitch_count = f" ({getattr(self, total)})" if include_pitch_count else ""
            pd_stats[metric] = f"{getattr(self, rate):.0%}{pitch_count}"
        return pd_stats

    def get_batted_ball_stats(self, include_bip_count: bool = False) -> Dict[str, str]:
        total_bip = f" ({self.total_balls_in_play})" if include_bip_count else ""
        bb_stats = {"pitch_type": f"{self.pitch_name}{total_bip}"}
        for metric, (rate, total) in PFX_BATTED_BALL_METRICS.items():
            bip_count = f" ({getattr(self, total)})" if include_bip_count else ""
            bb_stats[metric] = f"{getattr(self, rate):.0%}{bip_count}"
        return bb_stats

    def get_usage_stats(self, include_pitch_count: bool = False) -> str:
        pitch_count = f" ({self.total_pitches})" if include_pitch_count else ""
        return f"{self.percent:.0%}{pitch_count} {self.avg_speed:.1f}mph"

    @classmethod
    def from_query_results(cls, results: List[RowProxy]) -> List[PitchFxMetrics]:
        return [from_dict(data_class=cls, data=cls._get_pitchfx_metrics(dict(row))) for row in results]

    @classmethod
    def _get_pitchfx_metrics(cls, pfx_metrics: RowDict, decimal_precision: int = 3) -> PitchFxMetricsDict:
        dc_fields = get_field_types(cls)
        pitchfx_metrics_dict = {
            k: round(v, decimal_precision) if dc_fields[k] is float else bool(v) if dc_fields[k] is bool else v
            for k, v in pfx_metrics.items()
            if v and k in dc_fields
        }
        pitchfx_metrics_dict["pitch_type"] = (
            PitchType(pfx_metrics["pitch_type_int"])
            if "pitch_type_int" in pfx_metrics
            else PitchType.from_abbrev(pfx_metrics["pitch_type"])
            if "pitch_type" in pfx_metrics
            else None
        )
        pitchfx_metrics_dict["pitch_type_int"] = int(pitchfx_metrics_dict.get("pitch_type", 0))
        return pitchfx_metrics_dict


@dataclass
class PitchFxMetricsCollection(PitchFxMetrics):
    pitch_type_metrics: Dict[PitchType, PitchFxMetrics] = field(default_factory=dict)

    @property
    def pitch_name(self) -> str:
        return PitchType.ALL.print_name

    @property
    def pitch_types(self) -> List[PitchType]:
        return list(self.pitch_type_metrics.keys())

    @cached_property
    def pitch_mix(self) -> List[Dict[str, str]]:
        return [
            {"pitch_type": pitch_type.print_name, "usage": metrics.get_usage_stats()}
            for pitch_type, metrics in self.pitch_type_metrics.items()
        ]

    def get_bat_stats_pitch_type_splits(self, include_pa_count: bool = False) -> List[Dict[str, str]]:
        bat_stats_pt_splits = [metrics.get_bat_stats(include_pa_count) for metrics in self.pitch_type_metrics.values()]
        bat_stats_pt_splits.insert(0, self.get_bat_stats(include_pa_count))
        return bat_stats_pt_splits

    def get_plate_discipline_pitch_type_splits(self, include_pitch_count: bool = False) -> List[Dict[str, str]]:
        pd_split_stats = [
            metrics.get_plate_discipline_stats(include_pitch_count) for metrics in self.pitch_type_metrics.values()
        ]
        pd_split_stats.insert(0, self.get_plate_discipline_stats(include_pitch_count))
        return pd_split_stats

    def get_batted_ball_pitch_type_splits(self, include_bip_count: bool = False) -> List[Dict[str, str]]:
        bb_split_stats = [
            metrics.get_batted_ball_stats(include_bip_count) for metrics in self.pitch_type_metrics.values()
        ]
        bb_split_stats.insert(0, self.get_batted_ball_stats(include_bip_count))
        return bb_split_stats

    def get_usage_stats_for_pitch_type(self, pitch_type: PitchType, include_pitch_count: bool = False) -> str:
        return (
            self.pitch_type_metrics[pitch_type].get_usage_stats(include_pitch_count)
            if pitch_type in self.pitch_types
            else "0% (N/A)"
        )

    @classmethod
    def from_query_results(cls, all_pt_results: RowDict, each_pt_results: List[RowDict]) -> PitchFxMetricsCollection:
        metrics_collection = _get_pitchfx_metrics_for_all_pitch_types(all_pt_results)
        metrics_collection["pitch_type_metrics"] = _get_pitchfx_metrics_for_each_pitch_type(each_pt_results)
        metrics_collection["pitch_type_int"] = _get_all_pitch_types_int(metrics_collection["pitch_type_metrics"])
        metrics_collection["pitch_type"] = PitchType(metrics_collection["pitch_type_int"])
        return from_dict(data_class=cls, data=metrics_collection)


def _get_pitchfx_metrics_for_all_pitch_types(results: RowDict) -> PitchFxMetricsDict:
    metrics_collection = PitchFxMetrics._get_pitchfx_metrics(results)
    metrics_collection["percent"] = 1.0
    return metrics_collection


def _get_pitchfx_metrics_for_each_pitch_type(results: List[RowDict]) -> PitchTypeMetricsDict:
    all_pitch_types = [PitchFxMetrics._get_pitchfx_metrics(pitch_type_metrics) for pitch_type_metrics in results]
    total_pitches = sum(metrics["total_pitches"] for metrics in all_pitch_types)
    for pfx_metrics in all_pitch_types:
        pfx_metrics["percent"] = round(pfx_metrics["total_pitches"] / total_pitches, 3)
    all_pitch_types = list(filter(lambda x: x["percent"] >= 0.01, all_pitch_types))
    return {m["pitch_type"]: m for m in sorted(all_pitch_types, key=lambda x: x["percent"], reverse=True)}


def _get_all_pitch_types_int(pitch_type_metrics: PitchTypeMetricsDict) -> PitchType:
    return sum(int(pitch_type) for pitch_type in list(pitch_type_metrics.keys()))
