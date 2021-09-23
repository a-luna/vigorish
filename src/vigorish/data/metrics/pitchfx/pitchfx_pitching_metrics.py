from __future__ import annotations

from copy import deepcopy
from typing import List

import vigorish.database as db
from vigorish.data.metrics.pitchfx.pitchfx_metrics_set import (
    PfxMetricsSetBuilder,
    PitchFxMetricsSet,
)


class PitchFxPitchingMetrics:
    all: PitchFxMetricsSet
    rhb: PitchFxMetricsSet
    lhb: PitchFxMetricsSet

    def __init__(self, pfx: List[db.PitchFx], mlb_id: int, p_throws: str, remove_outliers: bool = False):
        pfx_metrics_builder = PfxMetricsSetBuilder()
        pfx_vs_rhb = list(filter(lambda x: x.stand == "R", pfx))
        pfx_vs_lhb = list(filter(lambda x: x.stand == "L", pfx))
        self.all = pfx_metrics_builder.create_pfx_metrics_set(
            deepcopy(pfx), mlb_id=mlb_id, p_throws=p_throws, remove_outliers=remove_outliers
        )
        self.rhb = pfx_metrics_builder.create_pfx_metrics_set(
            deepcopy(pfx_vs_rhb), mlb_id=mlb_id, p_throws=p_throws, bat_stand="R", remove_outliers=remove_outliers
        )
        self.lhb = pfx_metrics_builder.create_pfx_metrics_set(
            deepcopy(pfx_vs_lhb), mlb_id=mlb_id, p_throws=p_throws, bat_stand="L", remove_outliers=remove_outliers
        )

    def as_dict(self):
        return {
            "all": self.all.as_dict(),
            "rhb": self.rhb.as_dict(),
            "lhb": self.lhb.as_dict(),
        }
