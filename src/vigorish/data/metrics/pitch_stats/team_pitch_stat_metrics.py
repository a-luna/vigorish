from copy import deepcopy
from functools import cached_property
from typing import Dict, List

from sqlalchemy.orm import Session

import vigorish.database as db
from vigorish.data.metrics.pitch_stats import PitchStatsMetrics


class TeamPitchStatsMetrics:
    def __init__(self, db_session: Session, pitch_stats: List[db.PitchStats], team_id_bbref: str):
        self.db_session = db_session
        self.pitch_stats = pitch_stats
        self.team_id_bbref = team_id_bbref

    @cached_property
    def all_pitch_stats(self) -> PitchStatsMetrics:
        return self._create_pitch_stats_metrics(self.pitch_stats)

    @cached_property
    def for_sp(self) -> PitchStatsMetrics:
        sp_pitch_stats = self._get_sp_pitch_stats(self.pitch_stats)
        return self._create_pitch_stats_metrics(sp_pitch_stats, role="SP")

    @cached_property
    def for_rp(self) -> PitchStatsMetrics:
        rp_pitch_stats = self._get_rp_pitch_stats(self.pitch_stats)
        return self._create_pitch_stats_metrics(rp_pitch_stats, role="RP")

    @cached_property
    def by_year(self) -> Dict[int, PitchStatsMetrics]:
        all_season_ids = list({(stats.season.year, stats.season_id) for stats in self.pitch_stats})
        return {
            year: self._get_pitch_stat_metrics_for_season(self.pitch_stats, season_id, year)
            for year, season_id in sorted(all_season_ids, key=lambda x: x[0])
        }

    @cached_property
    def for_sp_by_year(self) -> Dict[int, PitchStatsMetrics]:
        all_season_ids = list({(stats.season.year, stats.season_id) for stats in self.pitch_stats})
        return {
            year: self._get_pitch_stat_metrics_for_sp_for_season(self.pitch_stats, season_id, year)
            for year, season_id in sorted(all_season_ids, key=lambda x: x[0])
        }

    @cached_property
    def for_rp_by_year(self) -> Dict[int, PitchStatsMetrics]:
        all_season_ids = list({(stats.season.year, stats.season_id) for stats in self.pitch_stats})
        return {
            year: self._get_pitch_stat_metrics_for_rp_for_season(self.pitch_stats, season_id, year)
            for year, season_id in sorted(all_season_ids, key=lambda x: x[0])
        }

    @cached_property
    def by_player_by_year(self) -> Dict[int, List[PitchStatsMetrics]]:
        all_season_ids = list({(stats.season.year, stats.season_id) for stats in self.pitch_stats})
        by_player_by_year = {}
        for year, season_id in sorted(all_season_ids, key=lambda x: x[0]):
            player_ids_for_season = list(
                {
                    (stats.player_id, stats.player_id_mlb, stats.player_id_bbref)
                    for stats in self.pitch_stats
                    if stats.season_id == season_id
                }
            )
            by_player_by_year[year] = [
                self._get_pitch_stat_metrics_for_player(self.pitch_stats, player_id, mlb_id, bbref_id, year, season_id)
                for player_id, mlb_id, bbref_id in player_ids_for_season
            ]
        return by_player_by_year

    @cached_property
    def for_sp_by_player_by_year(self) -> Dict[int, List[PitchStatsMetrics]]:
        all_season_ids = list({(stats.season.year, stats.season_id) for stats in self.pitch_stats})
        for_sp_by_player_by_year = {}
        for year, season_id in sorted(all_season_ids, key=lambda x: x[0]):
            player_ids_for_season = list(
                {
                    (stats.player_id, stats.player_id_mlb, stats.player_id_bbref)
                    for stats in self.pitch_stats
                    if stats.season_id == season_id and stats.is_sp == 1
                }
            )
            for_sp_by_player_by_year[year] = [
                self._get_pitch_stat_metrics_for_player_as_sp(
                    self.pitch_stats, player_id, mlb_id, bbref_id, year, season_id
                )
                for player_id, mlb_id, bbref_id in player_ids_for_season
            ]
        return for_sp_by_player_by_year

    @cached_property
    def for_rp_by_player_by_year(self) -> Dict[int, List[PitchStatsMetrics]]:
        all_season_ids = list({(stats.season.year, stats.season_id) for stats in self.pitch_stats})
        for_rp_by_player_by_year = {}
        for year, season_id in sorted(all_season_ids, key=lambda x: x[0]):
            player_ids_for_season = list(
                {
                    (stats.player_id, stats.player_id_mlb, stats.player_id_bbref)
                    for stats in self.pitch_stats
                    if stats.season_id == season_id and stats.is_rp == 1
                }
            )
            for_rp_by_player_by_year[year] = [
                self._get_pitch_stat_metrics_for_player_as_rp(
                    self.pitch_stats, player_id, mlb_id, bbref_id, year, season_id
                )
                for player_id, mlb_id, bbref_id in player_ids_for_season
            ]
        return for_rp_by_player_by_year

    def _get_pitch_stat_metrics_for_season(
        self, pitch_stats: List[db.PitchStats], season_id: int, year: int
    ) -> PitchStatsMetrics:
        pitch_stats_for_season = self._get_pitch_stats_for_season(season_id, pitch_stats)
        return self._create_pitch_stats_metrics(pitch_stats_for_season, year=year)

    def _get_pitch_stat_metrics_for_sp_for_season(
        self, pitch_stats: List[db.PitchStats], season_id: int, year: int
    ) -> PitchStatsMetrics:
        sp_pitch_stats_for_season = self._get_sp_pitch_stats(self._get_pitch_stats_for_season(season_id, pitch_stats))
        return self._create_pitch_stats_metrics(sp_pitch_stats_for_season, role="SP", year=year)

    def _get_pitch_stat_metrics_for_rp_for_season(
        self, pitch_stats: List[db.PitchStats], season_id: int, year: int
    ) -> PitchStatsMetrics:
        rp_pitch_stats_for_season = self._get_rp_pitch_stats(self._get_pitch_stats_for_season(season_id, pitch_stats))
        return self._create_pitch_stats_metrics(rp_pitch_stats_for_season, role="RP", year=year)

    def _get_pitch_stat_metrics_for_player(
        self, pitch_stats: List[db.PitchStats], player_id: int, mlb_id: int, bbref_id: str, year: int, season_id: int
    ) -> PitchStatsMetrics:
        pitch_stats_for_player = self._get_pitch_stats_for_player(
            player_id, self._get_pitch_stats_for_season(season_id, pitch_stats)
        )
        return self._create_pitch_stats_metrics(
            pitch_stats_for_player, player_id_mlb=mlb_id, player_id_bbref=bbref_id, year=year
        )

    def _get_pitch_stat_metrics_for_player_as_sp(
        self, pitch_stats: List[db.PitchStats], player_id: int, mlb_id: int, bbref_id: str, year: int, season_id: int
    ) -> PitchStatsMetrics:
        pitch_stats_for_player_as_sp = self._get_sp_pitch_stats(
            self._get_pitch_stats_for_player(player_id, self._get_pitch_stats_for_season(season_id, pitch_stats))
        )
        return self._create_pitch_stats_metrics(
            pitch_stats_for_player_as_sp, role="SP", player_id_mlb=mlb_id, player_id_bbref=bbref_id, year=year
        )

    def _get_pitch_stat_metrics_for_player_as_rp(
        self, pitch_stats: List[db.PitchStats], player_id: int, mlb_id: int, bbref_id: str, year: int, season_id: int
    ) -> PitchStatsMetrics:
        pitch_stats_for_player_as_rp = self._get_rp_pitch_stats(
            self._get_pitch_stats_for_player(player_id, self._get_pitch_stats_for_season(season_id, pitch_stats))
        )
        return self._create_pitch_stats_metrics(
            pitch_stats_for_player_as_rp, role="RP", player_id_mlb=mlb_id, player_id_bbref=bbref_id, year=year
        )

    def _get_sp_pitch_stats(self, pitch_stats: List[db.PitchStats]) -> List[db.PitchStats]:
        sp_pitch_stats = filter(lambda x: x.is_sp == 1, pitch_stats)
        return list(sp_pitch_stats)

    def _get_rp_pitch_stats(self, pitch_stats: List[db.PitchStats]) -> List[db.PitchStats]:
        rp_pitch_stats = filter(lambda x: x.is_rp == 1, pitch_stats)
        return list(rp_pitch_stats)

    def _get_pitch_stats_for_season(self, season_id: int, pitch_stats: List[db.PitchStats]) -> List[db.PitchStats]:
        pitch_stats_for_season = filter(lambda x: x.season_id == season_id, pitch_stats)
        return list(pitch_stats_for_season)

    def _get_pitch_stats_for_player(self, player_id: int, pitch_stats: List[db.PitchStats]) -> List[db.PitchStats]:
        pitch_stats_for_player = filter(lambda x: x.player_id == player_id, pitch_stats)
        return list(pitch_stats_for_player)

    def _create_pitch_stats_metrics(
        self,
        pitch_stats: List[db.PitchStats],
        role: str = None,
        year: int = None,
        player_id_mlb: int = None,
        player_id_bbref: str = None,
    ) -> PitchStatsMetrics:
        return PitchStatsMetrics(
            pitch_stats=deepcopy(pitch_stats),
            year=year,
            player_id_mlb=player_id_mlb,
            player_id_bbref=player_id_bbref,
            team_id_bbref=self.team_id_bbref,
            player_team_id_bbref=self.team_id_bbref,
            role=role,
        )


# import vigorish.database as db
# from vigorish.app import Vigorish
# from vigorish.data.player_data import PlayerData
# from vigorish.data.metrics.pitch_stats import PitchStatsMetricsFactory
# app = Vigorish()
# pitch_stats = PitchStatsMetricsFactory(app.db_session)
# t = pitch_stats.for_team("OAK")
