from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Dict, List, Tuple

import vigorish.database as db
from vigorish.data.metrics.pfx_metrics import PitchFxMetrics
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


class PfxMetricsFactory:
    def __init__(self, app):
        self.app = app
        self.db_session = app.db_session

    def for_pitcher_game(
        self, mlb_id: int, bbref_game_id: str, p_throws: str, remove_outliers: bool = False
    ) -> PitchFxPitchingMetrics:
        pitch_app_id = f"{bbref_game_id}_{mlb_id}"
        return self.for_pitch_app(pitch_app_id, p_throws, remove_outliers)

    def for_pitch_app(self, pitch_app_id: str, p_throws: str, remove_outliers: bool = False) -> PitchFxPitchingMetrics:
        pitch_app = db.PitchAppScrapeStatus.find_by_pitch_app_id(self.db_session, pitch_app_id)
        if not pitch_app:
            return None
        pfx = self.db_session.query(db.PitchFx).filter_by(pitch_app_db_id=pitch_app.id).all()
        return PitchFxPitchingMetrics(pfx, pitch_app.pitcher_id_mlb, p_throws, remove_outliers)

    def for_pitcher_season(
        self, mlb_id: int, year: int, p_throws: str, remove_outliers: bool = True
    ) -> PitchFxPitchingMetrics:
        pitcher = db.PlayerId.find_by_mlb_id(self.db_session, mlb_id)
        if not pitcher:
            return None
        season = db.Season.find_by_year(self.db_session, year)
        if not season:
            return None
        pfx = (
            self.db_session.query(db.PitchFx)
            .filter_by(pitcher_id=pitcher.db_player_id)
            .filter_by(season_id=season.id)
            .all()
        )
        return PitchFxPitchingMetrics(pfx, mlb_id, p_throws, remove_outliers)

    def for_pitcher_career(self, mlb_id: int, p_throws: str, remove_outliers: bool = True) -> PitchFxPitchingMetrics:
        pitcher = db.PlayerId.find_by_mlb_id(self.db_session, mlb_id)
        if not pitcher:
            return None
        pfx = self.db_session.query(db.PitchFx).filter_by(pitcher_id=pitcher.db_player_id).all()
        return PitchFxPitchingMetrics(pfx, mlb_id, p_throws, remove_outliers)

    def for_pitcher_by_year(
        self, mlb_id: int, p_throws: str, remove_outliers: bool = True
    ) -> Dict[int, PitchFxPitchingMetrics]:
        pitcher = db.PlayerId.find_by_mlb_id(self.db_session, mlb_id)
        if not pitcher:
            return None
        pfx = self.db_session.query(db.PitchFx).filter_by(pitcher_id=pitcher.db_player_id).all()
        season_ids = list({p.season_id for p in pfx})
        pfx_metrics_by_year = {}
        for sid in sorted(season_ids):
            season = self.db_session.query(db.Season).get(sid)
            pfx_for_season = list(filter(lambda x: x.season_id == sid, pfx))
            pfx_metrics_by_year[season.year] = PitchFxPitchingMetrics(
                deepcopy(pfx_for_season), mlb_id, p_throws, remove_outliers
            )
        return pfx_metrics_by_year

    def for_batter_game(self, mlb_id: int, bbref_game_id: str, remove_outliers: bool = False) -> PitchFxBattingMetrics:
        game_status = db.GameScrapeStatus.find_by_bbref_game_id(self.db_session, bbref_game_id)
        if not game_status:
            return None
        batter = db.PlayerId.find_by_mlb_id(self.db_session, mlb_id)
        if not batter:
            return None
        pfx = (
            self.db_session.query(db.PitchFx)
            .filter_by(game_status_id=game_status.id)
            .filter_by(batter_id=batter.db_player_id)
            .all()
        )
        return PitchFxBattingMetrics(pfx, mlb_id, remove_outliers)

    def for_batter_season(self, mlb_id: int, year: int, remove_outliers: bool = False) -> PitchFxBattingMetrics:
        season = db.Season.find_by_year(self.db_session, year)
        if not season:
            return None
        batter = db.PlayerId.find_by_mlb_id(self.db_session, mlb_id)
        if not batter:
            return None
        pfx = (
            self.db_session.query(db.PitchFx)
            .filter_by(batter_id=batter.db_player_id)
            .filter_by(season_id=season.id)
            .all()
        )
        return PitchFxBattingMetrics(pfx, mlb_id, remove_outliers)

    def for_batter_career(self, mlb_id: int, remove_outliers: bool = False) -> PitchFxBattingMetrics:
        batter = db.PlayerId.find_by_mlb_id(self.db_session, mlb_id)
        if not batter:
            return None
        pfx = self.db_session.query(db.PitchFx).filter_by(batter_id=batter.db_player_id).all()
        return PitchFxBattingMetrics(pfx, mlb_id, remove_outliers)

    def for_batter_by_year(self, mlb_id: int, remove_outliers: bool = False) -> Dict[int, PitchFxBattingMetrics]:
        batter = db.PlayerId.find_by_mlb_id(self.db_session, mlb_id)
        if not batter:
            return None
        pfx = self.db_session.query(db.PitchFx).filter_by(batter_id=batter.db_player_id).all()
        season_ids = list({p.season_id for p in pfx})
        pfx_metrics_by_year = {}
        for sid in season_ids:
            season = self.db_session.query(db.Season).get(sid)
            pfx_for_season = list(filter(lambda x: x.season_id == sid, pfx))
            pfx_metrics_by_year[season.year] = PitchFxBattingMetrics(pfx_for_season, mlb_id, remove_outliers)
        return pfx_metrics_by_year

    def for_team_pitching(self, team_id_br: str, year: int, remove_outliers: bool = True) -> PitchFxPitchingMetrics:
        team = db.Team.find_by_team_id_and_year(self.db_session, team_id_br, year)
        if not team:
            return None
        pfx = self.db_session.query(db.PitchFx).filter_by(team_pitching_id=team.id).all()
        return PitchFxPitchingMetrics(pfx, mlb_id=None, remove_outliers=remove_outliers)

    def for_team_batting(self, team_id_br: str, year: int, remove_outliers: bool = False) -> PitchFxPitchingMetrics:
        team = db.Team.find_by_team_id_and_year(self.db_session, team_id_br, year)
        if not team:
            return None
        pfx = self.db_session.query(db.PitchFx).filter_by(team_batting_id=team.id).all()
        return PitchFxBattingMetrics(pfx, mlb_id=None, remove_outliers=remove_outliers)
