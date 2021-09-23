from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Dict, List, Tuple

import vigorish.database as db
from vigorish.data.metrics.pitchfx.pitchfx_metrics import PitchFxMetrics
from vigorish.enums import PitchType

PitchFxByPitchTypeResults = Tuple[PitchFxMetrics, Dict[str, PitchFxMetrics], int, int]


@dataclass
class PitchFxMetricsSet:
    pitch_type_int: int
    total_pitches: int
    total_pfx_removed: int
    metrics_combined: PitchFxMetrics
    metrics_by_pitch_type: Dict[str, PitchFxMetrics]

    @property
    def pitch_type(self) -> List[PitchType]:
        return PitchType(self.pitch_type_int)

    @property
    def pitch_type_abbrevs(self) -> List[str]:
        return [str(pt) for pt in self.pitch_type]

    @property
    def pitch_mix(self) -> List[Dict[str, str]]:
        return [
            {
                "pitch_type": PitchType.from_abbrev(pitch_type).print_name,
                "usage": metrics.get_usage_stats(),
            }
            for pitch_type, metrics in self.metrics_by_pitch_type.items()
        ]

    def as_dict(self):
        return {
            "pitch_type": self.pitch_type_abbrevs,
            "pitch_type_int": self.pitch_type_int,
            "total_pitches": self.total_pitches,
            "total_pfx_removed": self.total_pfx_removed,
            "metrics_combined": self.metrics_combined.as_dict(),
            "metrics_by_pitch_type": {k: v.as_dict() for k, v in self.metrics_by_pitch_type.items()},
        }

    def get_bat_stats_pitch_type_splits(self, include_pa_count: bool = False) -> List[Dict[str, str]]:
        bat_stats_pt_splits = [
            metrics.get_bat_stats(include_pa_count) for metrics in self.metrics_by_pitch_type.values()
        ]
        bat_stats_pt_splits.insert(0, self.metrics_combined.get_bat_stats(include_pa_count))
        return bat_stats_pt_splits

    def get_plate_discipline_pitch_type_splits(self, include_pitch_count: bool = False) -> List[Dict[str, str]]:
        pd_split_stats = [
            metrics.get_plate_discipline_stats(include_pitch_count) for metrics in self.metrics_by_pitch_type.values()
        ]
        pd_split_stats.insert(0, self.metrics_combined.get_plate_discipline_stats(include_pitch_count))
        return pd_split_stats

    def get_batted_ball_pitch_type_splits(self, include_bip_count: bool = False) -> List[Dict[str, str]]:
        bb_split_stats = [
            metrics.get_batted_ball_stats(include_bip_count) for metrics in self.metrics_by_pitch_type.values()
        ]
        bb_split_stats.insert(0, self.metrics_combined.get_batted_ball_stats(include_bip_count))
        return bb_split_stats

    def get_usage_stats_for_pitch_type(self, pitch_type: PitchType, include_pitch_count: bool = False) -> str:
        return (
            self.metrics_by_pitch_type[str(pitch_type)].get_usage_stats(include_pitch_count)
            if pitch_type in self.pitch_type
            else "0% (N/A)"
        )


class PfxMetricsSetBuilder:
    def create_pfx_metrics_set(
        self,
        pfx: List[db.PitchFx],
        mlb_id: int = None,
        p_throws: str = None,
        bat_stand: str = None,
        remove_outliers: bool = False,
    ):
        self.pfx = pfx
        self.total_pfx = len(pfx)
        self.mlb_id = mlb_id
        self.p_throws = p_throws
        self.bat_stand = bat_stand
        self.remove_outliers = remove_outliers

        self._remove_invalid_data()
        (valid_pfx_metrics, outlier_pitch_types) = self._create_pfx_metrics_for_each_pitch_type()
        return self._create_pfx_metrics_set(valid_pfx_metrics, outlier_pitch_types)

    def _remove_invalid_data(self):
        self.pfx = list(filter(lambda x: not (x.is_invalid_ibb or x.is_out_of_sequence), self.pfx))

    def _create_pfx_metrics_for_each_pitch_type(self):
        all_pitch_types = PitchType(sum({p.pitch_type_int for p in self.pfx}))
        valid_pfx_metrics, outlier_pitch_types = [], []
        for pitch_type in all_pitch_types:
            pfx_for_pitch_type = list(filter(lambda x: x.mlbam_pitch_name == str(pitch_type), self.pfx))
            percent = round(len(pfx_for_pitch_type) / float(len(self.pfx)), 3)
            if self.remove_outliers and percent < 0.01:
                outlier_pitch_types.append(str(pitch_type))
                continue
            pfx_metrics = PitchFxMetrics(deepcopy(pfx_for_pitch_type), self.mlb_id, self.p_throws, self.bat_stand)
            valid_pfx_metrics.append(pfx_metrics)
        valid_pfx_metrics = self._sort_by_percent_thrown(valid_pfx_metrics)
        return (valid_pfx_metrics, outlier_pitch_types)

    def _sort_by_percent_thrown(self, pitch_type_metrics: List[PitchFxMetrics]) -> None:
        total_pitches = sum(metrics.total_pitches for metrics in pitch_type_metrics)
        for metrics in pitch_type_metrics:
            metrics.percent = metrics.total_pitches / float(total_pitches)
        return sorted(pitch_type_metrics, key=lambda x: x.percent, reverse=True)

    def _create_pfx_metrics_set(
        self, valid_pfx_metrics: List[PitchFxMetrics], outlier_pitch_types: List[str]
    ) -> PitchFxByPitchTypeResults:
        self.pfx = list(filter(lambda x: x.mlbam_pitch_name not in outlier_pitch_types, self.pfx))
        total_pfx_removed = self.total_pfx - len(self.pfx)

        metrics_by_pitch_type = {str(m.pitch_type): m for m in valid_pfx_metrics}
        metrics_combined = PitchFxMetrics(deepcopy(self.pfx), self.mlb_id, self.p_throws, self.bat_stand)
        metrics_combined.percent = 1
        return PitchFxMetricsSet(
            pitch_type_int=metrics_combined.pitch_type_int,
            total_pitches=metrics_combined.total_pitches,
            total_pfx_removed=total_pfx_removed,
            metrics_combined=metrics_combined,
            metrics_by_pitch_type=metrics_by_pitch_type,
        )
