from __future__ import annotations

from dataclasses import dataclass, field
from functools import cached_property
from typing import Dict, List, Union

from dacite import from_dict
from sqlalchemy.engine import RowProxy

from vigorish.data.metrics.constants import (
    PFX_BATTED_BALL_METRICS,
    PFX_BOOL_METRIC_NAMES,
    PFX_FLOAT_METRIC_NAMES,
    PFX_INT_METRIC_NAMES,
    PFX_PLATE_DISCIPLINE_METRICS,
)
from vigorish.enums import PitchType
from vigorish.types import RowDict


@dataclass
class PitchFxMetrics:
    pitch_type: PitchType = PitchType.UNKNOWN
    total_pitches: int = field(repr=False, default=0)
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
    total_batted_balls: int = field(repr=False, default=0)
    total_ground_balls: int = field(repr=False, default=0)
    total_line_drives: int = field(repr=False, default=0)
    total_fly_balls: int = field(repr=False, default=0)
    total_pop_ups: int = field(repr=False, default=0)
    percent: float = 0.0
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
    custom_score: float = field(repr=False, default=0.0)
    ground_ball_rate: float = field(repr=False, default=0.0)
    line_drive_rate: float = field(repr=False, default=0.0)
    fly_ball_rate: float = field(repr=False, default=0.0)
    pop_up_rate: float = field(repr=False, default=0.0)
    avg_speed: float = 0.0
    avg_pfx_x: float = field(repr=False, default=0.0)
    avg_pfx_z: float = field(repr=False, default=0.0)
    avg_px: float = field(repr=False, default=0.0)
    avg_pz: float = field(repr=False, default=0.0)
    money_pitch: bool = field(repr=False, default=False)

    def get_plate_discipline_stats(self, include_pitch_count: bool = False) -> Dict[str, str]:
        money_pitch = "$ " if self.money_pitch else ""
        total_pitches = f" ({self.total_pitches})" if include_pitch_count else ""
        pd_stats = {"pitch_type": f"{money_pitch}{self.pitch_type.print_name}{total_pitches}"}
        for metric, (rate, total) in PFX_PLATE_DISCIPLINE_METRICS.items():
            pitch_count = f" ({getattr(self, total)})" if include_pitch_count else ""
            pd_stats[metric] = f"{getattr(self, rate):.0%}{pitch_count}"
        return pd_stats

    def get_batted_ball_stats(self, include_bip_count: bool = False) -> Dict[str, str]:
        total_bip = f" ({self.total_batted_balls})" if include_bip_count else ""
        bb_stats = {"pitch_type": f"{self.pitch_type.print_name}{total_bip}"}
        for metric, (rate, total) in PFX_BATTED_BALL_METRICS.items():
            bip_count = f" ({getattr(self, total)})" if include_bip_count else ""
            bb_stats[metric] = f"{getattr(self, rate):.0%}{bip_count}"
        return bb_stats

    def get_usage_stats(self, include_pitch_count: bool = False) -> str:
        pitch_count = f" ({self.total_pitches})" if include_pitch_count else ""
        return f"{self.percent:.0%}{pitch_count} {self.avg_speed:.1f}mph"

    @classmethod
    def from_pitchfx_view_results(cls, row_dict: RowDict) -> PitchFxMetrics:
        return _get_pitchfx_metrics_for_single_pitch_type(row_dict)


@dataclass
class PitchFxMetricsCollection:
    pitcher_id_mlb: int
    pitch_types: List[PitchType] = field(repr=False, default_factory=list)
    total_pitches: int = field(repr=False, default=0)
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
    total_batted_balls: int = field(repr=False, default=0)
    total_ground_balls: int = field(repr=False, default=0)
    total_line_drives: int = field(repr=False, default=0)
    total_fly_balls: int = field(repr=False, default=0)
    total_pop_ups: int = field(repr=False, default=0)
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
    ground_ball_rate: float = field(repr=False, default=0.0)
    line_drive_rate: float = field(repr=False, default=0.0)
    fly_ball_rate: float = field(repr=False, default=0.0)
    pop_up_rate: float = field(repr=False, default=0.0)
    metrics_detail: Dict[PitchType, PitchFxMetrics] = field(default_factory=dict)

    @cached_property
    def pitch_mix(self) -> List[Dict[str, str]]:
        return [
            {"pitch_type": pitch_type.print_name, "usage": metrics.get_usage_stats()}
            for pitch_type, metrics in self.metrics_detail.items()
        ]

    def get_plate_discipline_stats_for_all_pitches(self, include_pitch_count: bool = False) -> Dict[str, str]:
        total_pitches = f" ({self.total_pitches})" if include_pitch_count else ""
        pd_stats = {"pitch_type": f"ALL{total_pitches}"}
        for metric, (rate, total) in PFX_PLATE_DISCIPLINE_METRICS.items():
            pitch_count = f" ({getattr(self, total)})" if include_pitch_count else ""
            pd_stats[metric] = f"{getattr(self, rate):.0%}{pitch_count}"
        return pd_stats

    def get_batted_ball_stats_for_all_pitches(self, include_bip_count: bool = False) -> Dict[str, str]:
        total_bip = f" ({self.total_batted_balls})" if include_bip_count else ""
        bb_stats = {"pitch_type": f"ALL{total_bip}"}
        for metric, (rate, total) in PFX_BATTED_BALL_METRICS.items():
            bip_count = f" ({getattr(self, total)})" if include_bip_count else ""
            bb_stats[metric] = f"{getattr(self, rate):.0%}{bip_count}"
        return bb_stats

    def get_plate_discipline_pitch_type_splits(self, include_pitch_count: bool = False) -> List[Dict[str, str]]:
        pd_split_stats = [
            metrics.get_plate_discipline_stats(include_pitch_count) for metrics in self.metrics_detail.values()
        ]
        pd_split_stats.insert(0, self.get_plate_discipline_stats_for_all_pitches(include_pitch_count))
        return pd_split_stats

    def get_batted_ball_pitch_type_splits(self, include_bip_count: bool = False) -> List[Dict[str, str]]:
        bb_split_stats = [metrics.get_batted_ball_stats(include_bip_count) for metrics in self.metrics_detail.values()]
        bb_split_stats.insert(0, self.get_batted_ball_stats_for_all_pitches(include_bip_count))
        return bb_split_stats

    def get_usage_stats_for_pitch_type(self, pitch_type: PitchType, include_pitch_count: bool = False) -> str:
        return (
            self.metrics_detail[pitch_type].get_usage_stats(include_pitch_count)
            if pitch_type in self.pitch_types
            else "0.0% (N/A)"
        )

    @classmethod
    def from_pitchfx_view_results(cls, results: List[RowProxy], threshold: float = 0.01) -> PitchFxMetricsCollection:
        row_dicts = [dict(row) for row in results]
        if not row_dicts:
            return None
        pitcher_id_mlb = row_dicts[0]["pitcher_id_mlb"]
        metrics_detail = _get_pitchfx_metrics_for_each_pitch_type(row_dicts, threshold)
        metrics_collection = _get_pitchfx_metrics_for_all_pitch_types(pitcher_id_mlb, metrics_detail)
        return from_dict(data_class=cls, data=metrics_collection)


PitchFxMetricsDict = Dict[str, Union[bool, int, float, str, PitchType]]
PitchTypeMetricsDict = Dict[PitchType, PitchFxMetricsDict]
PitchFxCollectionMetricsDict = Dict[str, Union[bool, int, float, str, List[PitchType]]]


def _get_pitchfx_metrics_for_each_pitch_type(row_dicts: List[RowDict], threshold: float) -> PitchTypeMetricsDict:
    valid_pitch_types = _filter_pitch_types_above_threshold(row_dicts, threshold)
    total_pitches_valid = sum(metrics["total_pitches"] for metrics in valid_pitch_types)
    for metrics in valid_pitch_types:
        metrics["percent"] = round(metrics["total_pitches"] / total_pitches_valid, 3)
    return {m["pitch_type"]: m for m in sorted(valid_pitch_types, key=lambda x: x["percent"], reverse=True)}


def _filter_pitch_types_above_threshold(row_dicts: List[RowDict], threshold: float) -> List[PitchFxMetricsDict]:
    all_pitch_types = [_get_pitchfx_metrics_for_single_pitch_type(d) for d in row_dicts]
    total_pitches = sum(metrics["total_pitches"] for metrics in all_pitch_types)
    return list(filter(lambda x: _check_threshold(x["total_pitches"], total_pitches, threshold), all_pitch_types))


def _get_pitchfx_metrics_for_single_pitch_type(pitch_type_dict: RowDict) -> PitchFxMetricsDict:
    pitch_metrics = {}
    pitch_metrics["pitch_type"] = PitchType.from_abbrev(pitch_type_dict["pitch_type"])
    for metric in PFX_FLOAT_METRIC_NAMES:
        if metric in pitch_type_dict:
            pitch_metrics[metric] = round(pitch_type_dict[metric], 3)
    for metric in PFX_INT_METRIC_NAMES:
        if metric in pitch_type_dict:
            pitch_metrics[metric] = pitch_type_dict[metric]
    for metric in PFX_BOOL_METRIC_NAMES:
        if metric in pitch_type_dict:
            pitch_metrics[metric] = bool(pitch_type_dict[metric])
    return pitch_metrics


def _check_threshold(pitch_count: int, total_pitches: int, threshold: float) -> bool:
    percent = pitch_count / float(total_pitches)
    return percent >= threshold


def _get_pitchfx_metrics_for_all_pitch_types(
    pitcher_id_mlb: int, metrics_detail: PitchTypeMetricsDict
) -> PitchFxCollectionMetricsDict:
    metrics_collection = {
        "pitcher_id_mlb": pitcher_id_mlb,
        "pitch_types": list(metrics_detail.keys()),
    }
    for metric_name in PFX_INT_METRIC_NAMES:
        metrics_collection[metric_name] = sum(
            pitch_metrics[metric_name]
            for pitch_metrics in metrics_detail.values()
            if metric_name in pitch_metrics and pitch_metrics[metric_name]
        )
    _calculate_pitch_discipline_metrics(metrics_collection)
    _calculate_batted_ball_metrics(metrics_collection)
    metrics_collection["metrics_detail"] = metrics_detail
    return metrics_collection


def _calculate_pitch_discipline_metrics(pitch_metrics: PitchFxCollectionMetricsDict) -> None:
    pitch_metrics["zone_rate"] = (
        round(pitch_metrics["total_inside_strike_zone"] / float(pitch_metrics["total_pitches"]), 3)
        if pitch_metrics["total_pitches"]
        else 0.0
    )
    pitch_metrics["called_strike_rate"] = (
        round(pitch_metrics["total_called_strikes"] / float(pitch_metrics["total_pitches"]), 3)
        if pitch_metrics["total_pitches"]
        else 0.0
    )
    pitch_metrics["swinging_strike_rate"] = (
        round(pitch_metrics["total_swinging_strikes"] / float(pitch_metrics["total_pitches"]), 3)
        if pitch_metrics["total_pitches"]
        else 0.0
    )
    pitch_metrics["whiff_rate"] = (
        round(pitch_metrics["total_swinging_strikes"] / float(pitch_metrics["total_swings"]), 3)
        if pitch_metrics["total_swings"]
        else 0.0
    )
    pitch_metrics["csw_rate"] = (
        round(
            (pitch_metrics["total_called_strikes"] + pitch_metrics["total_swinging_strikes"])
            / float(pitch_metrics["total_pitches"]),
            3,
        )
        if pitch_metrics["total_pitches"]
        else 0.0
    )
    pitch_metrics["o_swing_rate"] = (
        round(
            pitch_metrics["total_swings_outside_zone"] / float(pitch_metrics["total_outside_strike_zone"]),
            3,
        )
        if pitch_metrics["total_outside_strike_zone"]
        else 0.0
    )
    pitch_metrics["z_swing_rate"] = (
        round(
            pitch_metrics["total_swings_inside_zone"] / float(pitch_metrics["total_inside_strike_zone"]),
            3,
        )
        if pitch_metrics["total_inside_strike_zone"]
        else 0.0
    )
    pitch_metrics["swing_rate"] = (
        round(pitch_metrics["total_swings"] / float(pitch_metrics["total_pitches"]), 3)
        if pitch_metrics["total_pitches"]
        else 0.0
    )
    pitch_metrics["o_contact_rate"] = (
        round(
            pitch_metrics["total_contact_outside_zone"] / float(pitch_metrics["total_swings_outside_zone"]),
            3,
        )
        if pitch_metrics["total_swings_outside_zone"]
        else 0.0
    )
    pitch_metrics["z_contact_rate"] = (
        round(
            pitch_metrics["total_contact_inside_zone"] / float(pitch_metrics["total_swings_inside_zone"]),
            3,
        )
        if pitch_metrics["total_swings_inside_zone"]
        else 0.0
    )
    pitch_metrics["contact_rate"] = (
        round(pitch_metrics["total_swings_made_contact"] / float(pitch_metrics["total_pitches"]), 3)
        if pitch_metrics["total_pitches"]
        else 0.0
    )


def _calculate_batted_ball_metrics(pitch_metrics: PitchFxCollectionMetricsDict) -> None:
    pitch_metrics["ground_ball_rate"] = (
        round(pitch_metrics["total_ground_balls"] / float(pitch_metrics["total_batted_balls"]), 3)
        if pitch_metrics["total_batted_balls"]
        else 0.0
    )
    pitch_metrics["fly_ball_rate"] = (
        round(pitch_metrics["total_fly_balls"] / float(pitch_metrics["total_batted_balls"]), 3)
        if pitch_metrics["total_batted_balls"]
        else 0.0
    )
    pitch_metrics["line_drive_rate"] = (
        round(pitch_metrics["total_line_drives"] / float(pitch_metrics["total_batted_balls"]), 3)
        if pitch_metrics["total_batted_balls"]
        else 0.0
    )
    pitch_metrics["pop_up_rate"] = (
        round(pitch_metrics["total_pop_ups"] / float(pitch_metrics["total_batted_balls"]), 3)
        if pitch_metrics["total_batted_balls"]
        else 0.0
    )
