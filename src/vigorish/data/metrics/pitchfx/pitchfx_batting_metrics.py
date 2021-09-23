from __future__ import annotations

from copy import deepcopy
from typing import List

import vigorish.database as db
from vigorish.data.metrics.pitchfx.pitchfx_metrics_set import (
    PfxMetricsSetBuilder,
    PitchFxMetricsSet,
)


class PitchFxBattingMetrics:
    vs_all: PitchFxMetricsSet
    vs_rhp: PitchFxMetricsSet
    vs_lhp: PitchFxMetricsSet
    as_rhb_vs_rhp: PitchFxMetricsSet
    as_rhb_vs_lhp: PitchFxMetricsSet
    as_lhb_vs_rhp: PitchFxMetricsSet
    as_lhb_vs_lhp: PitchFxMetricsSet

    def __init__(self, pfx: List[db.PitchFx], mlb_id: int, remove_outliers: bool = False):
        pfx_metrics_builder = PfxMetricsSetBuilder()
        pfx_vs_rhp = list(filter(lambda x: x.p_throws == "R", pfx))
        pfx_vs_lhp = list(filter(lambda x: x.p_throws == "L", pfx))
        pfx_as_rhb_vs_rhp = list(filter(lambda x: (x.p_throws == "R" and x.stand == "R"), pfx))
        pfx_as_rhb_vs_lhp = list(filter(lambda x: (x.p_throws == "L" and x.stand == "R"), pfx))
        pfx_as_lhb_vs_rhp = list(filter(lambda x: (x.p_throws == "R" and x.stand == "L"), pfx))
        pfx_as_lhb_vs_lhp = list(filter(lambda x: (x.p_throws == "L" and x.stand == "L"), pfx))
        self.vs_all = pfx_metrics_builder.create_pfx_metrics_set(
            deepcopy(pfx), mlb_id=mlb_id, remove_outliers=remove_outliers
        )
        self.vs_rhp = pfx_metrics_builder.create_pfx_metrics_set(
            deepcopy(pfx_vs_rhp), mlb_id=mlb_id, p_throws="R", remove_outliers=remove_outliers
        )
        self.vs_lhp = pfx_metrics_builder.create_pfx_metrics_set(
            deepcopy(pfx_vs_lhp), mlb_id=mlb_id, p_throws="L", remove_outliers=remove_outliers
        )
        self.as_rhb_vs_rhp = pfx_metrics_builder.create_pfx_metrics_set(
            deepcopy(pfx_as_rhb_vs_rhp), mlb_id=mlb_id, p_throws="R", bat_stand="R", remove_outliers=remove_outliers
        )
        self.as_rhb_vs_lhp = pfx_metrics_builder.create_pfx_metrics_set(
            deepcopy(pfx_as_rhb_vs_lhp), mlb_id=mlb_id, p_throws="L", bat_stand="R", remove_outliers=remove_outliers
        )
        self.as_lhb_vs_rhp = pfx_metrics_builder.create_pfx_metrics_set(
            deepcopy(pfx_as_lhb_vs_rhp), mlb_id=mlb_id, p_throws="R", bat_stand="L", remove_outliers=remove_outliers
        )
        self.as_lhb_vs_lhp = pfx_metrics_builder.create_pfx_metrics_set(
            deepcopy(pfx_as_lhb_vs_lhp), mlb_id=mlb_id, p_throws="L", bat_stand="L", remove_outliers=remove_outliers
        )

    def as_dict(self):
        return {
            "vs_all": self.vs_all.as_dict(),
            "vs_rhp": self.vs_rhp.as_dict(),
            "vs_lhp": self.vs_lhp.as_dict(),
            "as_rhb_vs_rhp": self.as_rhb_vs_rhp.as_dict(),
            "as_rhb_vs_lhp": self.as_rhb_vs_lhp.as_dict(),
            "as_lhb_vs_rhp": self.as_lhb_vs_rhp.as_dict(),
            "as_lhb_vs_lhp": self.as_lhb_vs_lhp.as_dict(),
        }
