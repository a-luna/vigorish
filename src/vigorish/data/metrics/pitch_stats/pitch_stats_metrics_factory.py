from copy import deepcopy
from functools import cached_property
from typing import Dict, List

import vigorish.database as db
from vigorish.constants import TEAM_ID_MAP
from vigorish.data.metrics.pitch_stats import PitchStatsMetrics
from vigorish.data.metrics.pitch_stats.player_pitch_stat_metrics import PlayerPitchStatsMetrics
from vigorish.data.metrics.pitch_stats.team_pitch_stat_metrics import TeamPitchStatsMetrics


class PitchStatsMetricsFactory:
    def __init__(self, db_session):
        self.db_session = db_session
        self.player_cache: Dict[int, PlayerPitchStatsMetrics] = {}
        self.team_cache: Dict[int, TeamPitchStatsMetrics] = {}

    @cached_property
    def team_id_map(self):
        return {
            s.year: db.Team.get_team_id_map_for_year(self.db_session, s.year)
            for s in db.Season.get_all_regular_seasons(self.db_session)
        }

    def for_pitcher(self, mlb_id: int) -> PlayerPitchStatsMetrics:
        if mlb_id not in self.player_cache:
            self.player_cache[mlb_id] = self._get_pitch_stats_metrics_set_for_player(mlb_id)
        return self.player_cache[mlb_id]

    def for_team(self, team_id_bbref: str) -> TeamPitchStatsMetrics:
        if team_id_bbref not in self.team_cache:
            self.team_cache[team_id_bbref] = self._get_pitch_stats_metrics_set_for_team(team_id_bbref)
        return self.team_cache[team_id_bbref]

    def for_all_teams(self, year: int) -> List[PitchStatsMetrics]:
        return [
            PitchStatsMetrics(
                pitch_stats=deepcopy(self._get_pitch_stats_for_team(team_id, year)),
                year=year,
                team_id_bbref=team_id,
                player_team_id_bbref=team_id,
            )
            for team_id in list(TEAM_ID_MAP.keys())
        ]

    def for_sp_for_all_teams(self, year: int) -> List[PitchStatsMetrics]:
        return [
            PitchStatsMetrics(
                pitch_stats=deepcopy(self._get_pitch_stats_for_sp_for_team(team_id, year)),
                year=year,
                team_id_bbref=team_id,
                player_team_id_bbref=team_id,
                role="SP",
            )
            for team_id in list(TEAM_ID_MAP.keys())
        ]

    def for_rp_for_all_teams(self, year: int) -> List[PitchStatsMetrics]:
        return [
            PitchStatsMetrics(
                pitch_stats=deepcopy(self._get_pitch_stats_for_rp_for_team(team_id, year)),
                year=year,
                team_id_bbref=team_id,
                player_team_id_bbref=team_id,
                role="RP",
            )
            for team_id in list(TEAM_ID_MAP.keys())
        ]

    def _get_pitch_stats_metrics_set_for_team(self, team_id_bbref) -> TeamPitchStatsMetrics:
        return TeamPitchStatsMetrics(
            self.db_session, self._get_pitch_stats_for_team_franchise(team_id_bbref), team_id_bbref
        )

    def _get_pitch_stats_metrics_set_for_player(self, mlb_id) -> PlayerPitchStatsMetrics:
        pitcher = db.PlayerId.find_by_mlb_id(self.db_session, mlb_id)
        if not pitcher:
            return None
        pitch_stats = self.db_session.query(db.PitchStats).filter_by(player_id=pitcher.db_player_id).all()
        return PlayerPitchStatsMetrics(self.db_session, pitch_stats, mlb_id)

    def _get_pitch_stats_for_team_franchise(self, team_id_bbref: str) -> List[db.PitchStats]:
        return self.db_session.query(db.PitchStats).filter(db.PitchStats.player_team_id_bbref == team_id_bbref).all()

    def _get_pitch_stats_for_team(self, team_id_bbref: str, year: int) -> List[db.PitchStats]:
        return (
            self.db_session.query(db.PitchStats)
            .filter(db.PitchStats.player_team_id == self.team_id_map[year][team_id_bbref])
            .all()
        )

    def _get_pitch_stats_for_sp_for_team(self, team_id_bbref: str, year: int) -> List[db.PitchStats]:
        return (
            self.db_session.query(db.PitchStats)
            .filter(db.PitchStats.player_team_id == self.team_id_map[year][team_id_bbref])
            .filter(db.PitchStats.is_sp == 1)
            .all()
        )

    def _get_pitch_stats_for_rp_for_team(self, team_id_bbref: str, year: int) -> List[db.PitchStats]:
        return (
            self.db_session.query(db.PitchStats)
            .filter(db.PitchStats.player_team_id == self.team_id_map[year][team_id_bbref])
            .filter(db.PitchStats.is_rp == 1)
            .all()
        )
