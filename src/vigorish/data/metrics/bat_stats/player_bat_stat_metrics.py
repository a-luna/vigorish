from copy import deepcopy
from functools import cached_property
from typing import Dict, List

from sqlalchemy.orm import Session

import vigorish.database as db
from vigorish.data.metrics.bat_stats.bat_stats_metrics import BatStatsMetrics


class PlayerBatStatsMetrics:
    def __init__(self, db_session: Session, bat_stats: List[db.BatStats], mlb_id: int):
        self.db_session = db_session
        self.bat_stats = bat_stats
        self.mlb_id = mlb_id

    @cached_property
    def team_id_map(self):
        return {
            s.year: db.Team.get_team_id_map_for_year(self.db_session, s.year)
            for s in db.Season.get_all_regular_seasons(self.db_session)
        }

    @cached_property
    def player_id_bbref(self) -> str:
        player_id = db.PlayerId.find_by_mlb_id(self.db_session, self.mlb_id)
        return player_id.bbref_id if player_id else ""

    @cached_property
    def all_bat_stats(self) -> BatStatsMetrics:
        return BatStatsMetrics(
            bat_stats=deepcopy(self.bat_stats),
            player_id_mlb=self.mlb_id,
            player_id_bbref=self.player_id_bbref,
        )

    @cached_property
    def by_year(self) -> Dict[int, BatStatsMetrics]:
        all_seasons = list({(stats.season.year, stats.season_id) for stats in self.bat_stats})
        return [
            self._get_bat_stat_metrics_for_season(season_id, year)
            for year, season_id in sorted(all_seasons, key=lambda x: x[0])
        ]

    @cached_property
    def by_team(self) -> Dict[str, BatStatsMetrics]:
        all_teams = list({stats.player_team_id_bbref for stats in self.bat_stats})
        return [self._get_bat_stat_metrics_for_team(team_id_bbref) for team_id_bbref in sorted(all_teams)]

    @cached_property
    def by_team_by_year(self) -> Dict[int, Dict[str, BatStatsMetrics]]:
        all_seasons = list({stats.season.year for stats in self.bat_stats})
        by_team_by_year = []
        for year in sorted(all_seasons):
            stints_for_year = db.Assoc_Player_Team.get_stints_for_year_for_player(self.db_session, self.mlb_id, year)
            stints_for_year = [
                self._get_bat_stat_metrics_for_team_for_season(
                    self.team_id_map[year][stint["team_id"]], stint["team_id"], year, stint["stint_number"]
                )
                for stint in stints_for_year
            ]
            by_team_by_year.extend(stints_for_year)
        return by_team_by_year

    @cached_property
    def by_opponent(self) -> Dict[str, BatStatsMetrics]:
        all_opponents = list({stats.opponent_team_id_bbref for stats in self.bat_stats})
        return {
            team_id_bbref: self._get_bat_stat_metrics_vs_team(team_id_bbref) for team_id_bbref in sorted(all_opponents)
        }

    @cached_property
    def by_opponent_by_year(self) -> Dict[str, BatStatsMetrics]:
        all_seasons = list({(stats.season.year, stats.season_id) for stats in self.bat_stats})
        by_opponent_by_year = {}
        for year, season_id in sorted(all_seasons, key=lambda x: x[0]):
            all_opponents_for_season = list(
                {
                    (stats.opponent_team_id_bbref, stats.opponent_team_id)
                    for stats in self.bat_stats
                    if stats.season_id == season_id
                }
            )
            by_opponent_by_year[year] = {
                team_id_bbref: self._get_bat_stat_metrics_vs_team_for_season(team_id, team_id_bbref, year)
                for team_id_bbref, team_id in sorted(all_opponents_for_season, key=lambda x: x[0])
            }
        return by_opponent_by_year

    def _get_bat_stat_metrics_for_season(self, season_id: int, year: int) -> BatStatsMetrics:
        bat_stats_for_season = deepcopy(list(filter(lambda x: x.season_id == season_id, self.bat_stats)))
        return BatStatsMetrics(
            bat_stats=bat_stats_for_season,
            year=year,
            player_id_mlb=self.mlb_id,
            player_id_bbref=self.player_id_bbref,
        )

    def _get_bat_stat_metrics_for_team(self, team_id_bbref: str) -> BatStatsMetrics:
        bat_stats_for_team = deepcopy(list(filter(lambda x: x.player_team_id_bbref == team_id_bbref, self.bat_stats)))
        return BatStatsMetrics(
            bat_stats=bat_stats_for_team,
            player_id_mlb=self.mlb_id,
            player_id_bbref=self.player_id_bbref,
            player_team_id_bbref=team_id_bbref,
        )

    def _get_bat_stat_metrics_for_team_for_season(
        self, team_id: int, team_id_bbref: str, year: int, stint_number: int
    ) -> BatStatsMetrics:
        bat_stats_for_team = deepcopy(list(filter(lambda x: x.player_team_id == team_id, self.bat_stats)))
        return BatStatsMetrics(
            bat_stats=bat_stats_for_team,
            year=year,
            player_id_mlb=self.mlb_id,
            player_id_bbref=self.player_id_bbref,
            player_team_id_bbref=team_id_bbref,
            stint_number=stint_number,
        )

    def _get_bat_stat_metrics_vs_team(self, team_id_bbref: str):
        bat_stats_vs_team = deepcopy(list(filter(lambda x: x.opponent_team_id_bbref == team_id_bbref, self.bat_stats)))
        return BatStatsMetrics(
            bat_stats=bat_stats_vs_team,
            player_id_mlb=self.mlb_id,
            player_id_bbref=self.player_id_bbref,
            opponent_team_id_bbref=team_id_bbref,
        )

    def _get_bat_stat_metrics_vs_team_for_season(self, team_id: int, team_id_bbref: str, year: int) -> BatStatsMetrics:
        bat_stats_vs_team = deepcopy(list(filter(lambda x: x.opponent_team_id == team_id, self.bat_stats)))
        return BatStatsMetrics(
            bat_stats=bat_stats_vs_team,
            year=year,
            player_id_mlb=self.mlb_id,
            player_id_bbref=self.player_id_bbref,
            opponent_team_id_bbref=team_id_bbref,
        )

    def _get_player_team_date_intervals(self, year: int):
        total_pitch_stats_for_season = (
            self.db_session.query(db.BatStats).filter(db.BatStats.player_id_mlb == self.mlb_id).count()
        )
        total_bat_stats_for_season = (
            self.db_session.query(db.BatStats).filter(db.BatStats.player_id_mlb == self.mlb_id).count()
        )
        if total_pitch_stats_for_season > total_bat_stats_for_season:
            return db.BatStats.get_date_intervals_for_player_teams(self.db_session, self.mlb_id, year)
        return db.BatStats.get_date_intervals_for_player_teams(self.db_session, self.mlb_id, year)
