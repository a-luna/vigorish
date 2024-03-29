from copy import deepcopy
from functools import cached_property

import vigorish.database as db
from vigorish.constants import TEAM_ID_MAP
from vigorish.data.metrics.bat_stats.bat_stats_metrics import BatStatsMetrics
from vigorish.data.metrics.bat_stats.player_bat_stat_metrics import PlayerBatStatsMetrics
from vigorish.data.metrics.bat_stats.team_bat_stat_metrics import TeamBatStatsMetrics
from vigorish.enums import DefensePosition


class BatStatsMetricsFactory:
    def __init__(self, db_session):
        self.db_session = db_session
        self.player_cache: dict[int, PlayerBatStatsMetrics] = {}
        self.team_cache: dict[int, TeamBatStatsMetrics] = {}

    @cached_property
    def team_id_map(self):
        return {
            s.year: db.Team.get_team_id_map_for_year(self.db_session, s.year)
            for s in db.Season.get_all_regular_seasons(self.db_session)
        }

    def for_player(self, mlb_id: int) -> PlayerBatStatsMetrics:
        if mlb_id not in self.player_cache:
            self.player_cache[mlb_id] = self._get_bat_stats_metrics_set_for_player(mlb_id)
        return self.player_cache[mlb_id]

    def for_team(self, team_id_bbref: str) -> TeamBatStatsMetrics:
        if team_id_bbref not in self.team_cache:
            self.team_cache[team_id_bbref] = self._get_bat_stats_metrics_set_for_team(team_id_bbref)
        return self.team_cache[team_id_bbref]

    def for_all_teams(self, year: int) -> list[BatStatsMetrics]:
        return [
            BatStatsMetrics(
                bat_stats=deepcopy(self._get_bat_stats_for_team(team_id, year)),
                year=year,
                team_id_bbref=team_id,
                player_team_id_bbref=team_id,
            )
            for team_id in list(TEAM_ID_MAP.keys())
        ]

    def for_starters_for_all_teams(self, year: int) -> list[BatStatsMetrics]:
        return [
            BatStatsMetrics(
                bat_stats=deepcopy(self._get_bat_stats_for_starters_for_team(team_id, year)),
                year=year,
                team_id_bbref=team_id,
                player_team_id_bbref=team_id,
                is_starter=True,
            )
            for team_id in list(TEAM_ID_MAP.keys())
        ]

    def for_bench_for_all_teams(self, year: int) -> list[BatStatsMetrics]:
        return [
            BatStatsMetrics(
                bat_stats=deepcopy(self._get_bat_stats_for_bench_for_team(team_id, year)),
                year=year,
                team_id_bbref=team_id,
                player_team_id_bbref=team_id,
                is_starter=False,
            )
            for team_id in list(TEAM_ID_MAP.keys())
        ]

    def for_lineup_spots_for_all_teams(self, bat_order_list: list[int], year: int) -> list[BatStatsMetrics]:
        return [
            BatStatsMetrics(
                bat_stats=deepcopy(self._get_bat_stats_for_lineup_spots_for_team(bat_order_list, team_id, year)),
                year=year,
                team_id_bbref=team_id,
                player_team_id_bbref=team_id,
            )
            for team_id in list(TEAM_ID_MAP.keys())
        ]

    def for_def_positions_for_all_teams(
        self,
        def_position_list: list[DefensePosition],
        year: int,
    ) -> list[BatStatsMetrics]:
        return [
            BatStatsMetrics(
                bat_stats=deepcopy(self._get_bat_stats_for_def_positions_for_team(def_position_list, team_id, year)),
                year=year,
                team_id_bbref=team_id,
                player_team_id_bbref=team_id,
            )
            for team_id in list(TEAM_ID_MAP.keys())
        ]

    def _get_bat_stats_metrics_set_for_team(self, team_id_bbref) -> TeamBatStatsMetrics:
        return TeamBatStatsMetrics(
            self.db_session, self._get_bat_stats_for_team_franchise(team_id_bbref), team_id_bbref
        )

    def _get_bat_stats_metrics_set_for_player(self, mlb_id) -> PlayerBatStatsMetrics:
        player = db.PlayerId.find_by_mlb_id(self.db_session, mlb_id)
        if not player:
            return None
        bat_stats = self.db_session.query(db.BatStats).filter_by(player_id=player.db_player_id).all()
        return PlayerBatStatsMetrics(self.db_session, bat_stats, mlb_id)

    def _get_bat_stats_for_team_franchise(self, team_id_bbref: str) -> list[db.BatStats]:
        return self.db_session.query(db.BatStats).filter(db.BatStats.player_team_id_bbref == team_id_bbref).all()

    def _get_bat_stats_for_team(self, team_id_bbref: str, year: int) -> list[db.BatStats]:
        return (
            self.db_session.query(db.BatStats)
            .filter(db.BatStats.player_team_id == self.team_id_map[year][team_id_bbref])
            .all()
        )

    def _get_bat_stats_for_starters_for_team(self, team_id_bbref: str, year: int) -> list[db.BatStats]:
        return (
            self.db_session.query(db.BatStats)
            .filter(db.BatStats.player_team_id == self.team_id_map[year][team_id_bbref])
            .filter(db.BatStats.is_starter == 1)
            .all()
        )

    def _get_bat_stats_for_bench_for_team(self, team_id_bbref: str, year: int) -> list[db.BatStats]:
        return (
            self.db_session.query(db.BatStats)
            .filter(db.BatStats.player_team_id == self.team_id_map[year][team_id_bbref])
            .filter(db.BatStats.is_starter == 0)
            .all()
        )

    def _get_bat_stats_for_lineup_spots_for_team(
        self, bat_order_list: list[int], team_id_bbref: str, year: int
    ) -> list[db.BatStats]:
        team_bat_stats = (
            self.db_session.query(db.BatStats)
            .filter(db.BatStats.player_team_id == self.team_id_map[year][team_id_bbref])
            .all()
        )
        return [bat_stats for bat_stats in team_bat_stats if bat_stats.bat_order in bat_order_list]

    def _get_bat_stats_for_def_positions_for_team(
        self, def_position_list: list[DefensePosition], team_id_bbref: str, year: int
    ) -> list[db.BatStats]:
        team_bat_stats = (
            self.db_session.query(db.BatStats)
            .filter(db.BatStats.player_team_id == self.team_id_map[year][team_id_bbref])
            .all()
        )
        return [
            bat_stats
            for bat_stats in team_bat_stats
            if bat_stats.def_position in _convert_def_position_list_to_str_list(def_position_list)
        ]


def _convert_def_position_list_to_str_list(def_positions: list[DefensePosition]) -> list[str]:
    return [str(int(def_pos)) for def_pos in def_positions]
