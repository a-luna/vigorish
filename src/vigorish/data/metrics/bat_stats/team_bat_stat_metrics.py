from collections import defaultdict
from copy import deepcopy
from functools import cached_property
from typing import Dict, List

from sqlalchemy.orm import Session

import vigorish.database as db
from vigorish.data.metrics.bat_stats.bat_stats_metrics import BatStatsMetrics
from vigorish.enums import DefensePosition


class TeamBatStatsMetrics:
    def __init__(self, db_session: Session, bat_stats: List[db.BatStats], team_id_bbref: str):
        self.db_session = db_session
        self.bat_stats = bat_stats
        self.team_id_bbref = team_id_bbref

    @cached_property
    def season_id_map(self):
        return {s.year: s.id for s in db.Season.get_all_regular_seasons(self.db_session)}

    @cached_property
    def all_pitch_stats(self) -> BatStatsMetrics:
        return create_bat_stats_metrics(team_id=self.team_id_bbref, bat_stats=self.bat_stats)

    @cached_property
    def by_year(self) -> List[BatStatsMetrics]:
        all_season_ids = list({(stats.season.year, stats.season_id) for stats in self.bat_stats})
        return {
            year: self._get_bat_stat_metrics_for_season(self.bat_stats, season_id, year)
            for year, season_id in sorted(all_season_ids, key=lambda x: x[0])
        }

    @cached_property
    def for_starters_by_year(self) -> List[BatStatsMetrics]:
        all_season_ids = list({(stats.season.year, stats.season_id) for stats in self.bat_stats})
        return {
            year: self._get_bat_stat_metrics_for_starters(self.bat_stats, season_id, year)
            for year, season_id in sorted(all_season_ids, key=lambda x: x[0])
        }

    @cached_property
    def for_bench_by_year(self) -> Dict[int, BatStatsMetrics]:
        all_season_ids = list({(stats.season.year, stats.season_id) for stats in self.bat_stats})
        return {
            year: self._get_bat_stat_metrics_for_bench(self.bat_stats, season_id, year)
            for year, season_id in sorted(all_season_ids, key=lambda x: x[0])
        }

    @cached_property
    def by_lineup_spot_by_year(self) -> Dict[int, List[BatStatsMetrics]]:
        all_season_ids = list({(stats.season.year, stats.season_id) for stats in self.bat_stats})
        by_lineup_spot_by_year = {}
        for year, season_id in sorted(all_season_ids, key=lambda x: x[0]):
            lineup_spots_for_season = list(
                {stats.bat_order for stats in self.bat_stats if stats.season_id == season_id}
            )
            by_lineup_spot_by_year[year] = [
                self._get_bat_stat_metrics_for_lineup_spots(self.bat_stats, [bat_order], season_id, year)
                for bat_order in lineup_spots_for_season
            ]
        return by_lineup_spot_by_year

    @cached_property
    def by_def_position_by_year(self) -> Dict[int, List[BatStatsMetrics]]:
        all_season_ids = list({(stats.season.year, stats.season_id) for stats in self.bat_stats})
        by_def_position_by_year = {}
        for year, season_id in sorted(all_season_ids, key=lambda x: x[0]):
            def_positions_for_season = list(
                {stats.def_position for stats in self.bat_stats if stats.season_id == season_id}
            )
            by_def_position_by_year[year] = [
                self._get_bat_stat_metrics_for_def_positions(
                    self.bat_stats, [DefensePosition(int(def_pos))], season_id, year
                )
                for def_pos in def_positions_for_season
            ]
        return by_def_position_by_year

    @cached_property
    def by_player_by_year(self) -> Dict[int, List[BatStatsMetrics]]:
        all_season_ids = list({(stats.season.year, stats.season_id) for stats in self.bat_stats})
        by_player_by_year = {}
        for year, season_id in sorted(all_season_ids, key=lambda x: x[0]):
            player_ids_for_season = list(
                {
                    (stats.player_id, stats.player_id_mlb, stats.player_id_bbref)
                    for stats in self.bat_stats
                    if stats.season_id == season_id
                }
            )
            by_player_by_year[year] = [
                self._get_bat_stat_metrics_for_player(self.bat_stats, player_id, mlb_id, bbref_id, year, season_id)
                for player_id, mlb_id, bbref_id in player_ids_for_season
            ]
        return by_player_by_year

    @cached_property
    def by_lineup_spot_by_player_by_year(self) -> Dict[int, Dict[int, List[BatStatsMetrics]]]:
        all_season_ids = list({(stats.season.year, stats.season_id) for stats in self.bat_stats})
        by_player_by_lineup_spot_by_year = defaultdict(dict)
        for year, season_id in sorted(all_season_ids, key=lambda x: x[0]):
            lineup_spots_for_season = list(
                {stats.bat_order for stats in self.bat_stats if stats.season_id == season_id}
            )
            for bat_order in lineup_spots_for_season:
                player_ids_for_lineup_spot_for_season = list(
                    {
                        (stats.player_id, stats.player_id_mlb, stats.player_id_bbref)
                        for stats in self.bat_stats
                        if stats.season_id == season_id and stats.bat_order == bat_order
                    }
                )
                by_player_by_lineup_spot_by_year[year][bat_order] = [
                    self.get_bat_stat_metrics_for_lineup_spots_for_player(
                        self.bat_stats, [bat_order], player_id, player_id_mlb, player_id_bbref, year, season_id
                    )
                    for player_id, player_id_mlb, player_id_bbref in player_ids_for_lineup_spot_for_season
                ]
        return by_player_by_lineup_spot_by_year

    @cached_property
    def by_def_position_by_player_by_year(self) -> Dict[int, Dict[DefensePosition, List[BatStatsMetrics]]]:
        all_season_ids = list({(stats.season.year, stats.season_id) for stats in self.bat_stats})
        by_player_by_def_position_by_year = defaultdict(dict)
        for year, season_id in sorted(all_season_ids, key=lambda x: x[0]):
            def_positions_for_season = list(
                {stats.def_position for stats in self.bat_stats if stats.season_id == season_id}
            )
            for def_pos in def_positions_for_season:
                player_ids_for_def_position_for_season = list(
                    {
                        (stats.player_id, stats.player_id_mlb, stats.player_id_bbref)
                        for stats in self.bat_stats
                        if stats.season_id == season_id and stats.def_position == def_pos
                    }
                )
                by_player_by_def_position_by_year[year][def_pos] = [
                    self.get_bat_stat_metrics_for_def_positions_for_player(
                        self.bat_stats,
                        [DefensePosition(int(def_pos))],
                        player_id,
                        player_id_mlb,
                        player_id_bbref,
                        year,
                        season_id,
                    )
                    for player_id, player_id_mlb, player_id_bbref in player_ids_for_def_position_for_season
                ]
        return by_player_by_def_position_by_year

    def for_lineup_spots_by_year(self, bat_order_list: List[int]) -> Dict[int, BatStatsMetrics]:
        all_season_ids = list({(stats.season.year, stats.season_id) for stats in self.bat_stats})
        return {
            year: self._get_bat_stat_metrics_for_lineup_spots(self.bat_stats, bat_order_list, season_id, year)
            for year, season_id in all_season_ids
        }

    def for_def_positions_by_year(self, def_position_list: List[DefensePosition]) -> Dict[int, BatStatsMetrics]:
        all_season_ids = list({(stats.season.year, stats.season_id) for stats in self.bat_stats})
        return {
            year: self._get_bat_stat_metrics_for_def_positions(self.bat_stats, def_position_list, season_id, year)
            for year, season_id in all_season_ids
        }

    def for_starters_by_player_for_year(self, year: int) -> List[BatStatsMetrics]:
        all_player_ids = list(
            {
                (stats.player_id, stats.player_id_mlb, stats.player_id_bbref)
                for stats in self.bat_stats
                if stats.season_id == self.season_id_map[year] and stats.is_starter == 1
            }
        )
        return [
            self._get_bat_stat_metrics_when_starter_for_player(
                self.bat_stats, player_id, mlb_id, bbref_id, year, self.season_id_map[year]
            )
            for player_id, mlb_id, bbref_id in all_player_ids
        ]

    def for_bench_by_player_for_year(self, year: int) -> List[BatStatsMetrics]:
        all_player_ids = list(
            {
                (stats.player_id, stats.player_id_mlb, stats.player_id_bbref)
                for stats in self.bat_stats
                if stats.season_id == self.season_id_map[year] and stats.is_starter == 1
            }
        )
        return [
            self._get_bat_stat_metrics_when_bench_for_player(
                self.bat_stats, player_id, mlb_id, bbref_id, year, self.season_id_map[year]
            )
            for player_id, mlb_id, bbref_id in all_player_ids
        ]

    def for_lineup_spots_by_player_for_year(self, bat_order_list: List[int], year: int) -> List[BatStatsMetrics]:
        all_player_ids = list(
            {
                (stats.player_id, stats.player_id_mlb, stats.player_id_bbref)
                for stats in self.bat_stats
                if stats.season_id == self.season_id_map[year] and stats.bat_order in bat_order_list
            }
        )

        team_bat_stats_for_lineup_spots = self._get_bat_stat_metrics_for_lineup_spots(
            self.bat_stats, bat_order_list, self.season_id_map[year], year
        )
        combined_metrics_by_player = [
            self.get_bat_stat_metrics_for_lineup_spots_for_player(
                self.bat_stats,
                bat_order_list,
                player_id,
                mlb_id,
                bbref_id,
                year,
                self.season_id_map[year],
            )
            for player_id, mlb_id, bbref_id in all_player_ids
        ]
        combined_metrics_by_player.sort(key=lambda x: x.total_games, reverse=True)
        separate_metrics_by_player = {
            mlb_id: [
                self.get_bat_stat_metrics_for_lineup_spots_for_player(
                    self.bat_stats,
                    [bat_order],
                    player_id,
                    mlb_id,
                    bbref_id,
                    year,
                    self.season_id_map[year],
                )
                for bat_order in bat_order_list
            ]
            for player_id, mlb_id, bbref_id in all_player_ids
        }
        combined_and_separate_metrics = []
        for combined_metrics in combined_metrics_by_player:
            combined_metrics = set_flag_for_combined_bat_order_metrics(combined_metrics)
            separate_metrics = separate_metrics_by_player.get(combined_metrics.mlb_id, [])
            separate_metrics = list(filter(lambda x: x.total_games > 0, separate_metrics))
            separate_metrics = list(map(set_flag_for_separate_bat_order_metrics, separate_metrics))
            separate_metrics.sort(key=lambda x: x.total_games, reverse=True)
            combined_and_separate_metrics.append(combined_metrics)
            combined_and_separate_metrics.extend(separate_metrics)
        combined_and_separate_metrics.append(team_bat_stats_for_lineup_spots)
        return combined_and_separate_metrics

    def for_def_positions_by_player_for_year(
        self, def_position_list: List[DefensePosition], year: int
    ) -> List[BatStatsMetrics]:
        all_player_ids = list(
            {
                (stats.player_id, stats.player_id_mlb, stats.player_id_bbref)
                for stats in self.bat_stats
                if stats.season_id == self.season_id_map[year]
                and stats.def_position in convert_def_position_list_to_str_list(def_position_list)
            }
        )
        team_bat_stats_for_def_positions = self._get_bat_stat_metrics_for_def_positions(
            self.bat_stats, def_position_list, self.season_id_map[year], year
        )
        combined_metrics_by_player = [
            self.get_bat_stat_metrics_for_def_positions_for_player(
                self.bat_stats,
                def_position_list,
                player_id,
                mlb_id,
                bbref_id,
                year,
                self.season_id_map[year],
            )
            for player_id, mlb_id, bbref_id in all_player_ids
        ]
        combined_metrics_by_player.sort(key=lambda x: x.total_games, reverse=True)
        separate_metrics_by_player = {
            mlb_id: [
                self.get_bat_stat_metrics_for_def_positions_for_player(
                    self.bat_stats,
                    [def_pos],
                    player_id,
                    mlb_id,
                    bbref_id,
                    year,
                    self.season_id_map[year],
                )
                for def_pos in def_position_list
            ]
            for player_id, mlb_id, bbref_id in all_player_ids
        }
        combined_and_separate_metrics = []
        for combined_metrics in combined_metrics_by_player:
            combined_metrics = set_flag_for_combined_def_pos_metrics(combined_metrics)
            separate_metrics = separate_metrics_by_player.get(combined_metrics.mlb_id, [])
            separate_metrics = list(filter(lambda x: x.total_games > 0, separate_metrics))
            separate_metrics = list(map(set_flag_for_separate_def_pos_metrics, separate_metrics))
            separate_metrics.sort(key=lambda x: x.total_games, reverse=True)
            combined_and_separate_metrics.append(combined_metrics)
            combined_and_separate_metrics.extend(separate_metrics)
        combined_and_separate_metrics.append(team_bat_stats_for_def_positions)
        return combined_and_separate_metrics

    def _get_bat_stat_metrics_for_season(
        self, bat_stats: List[db.BatStats], season_id: int, year: int
    ) -> BatStatsMetrics:
        bat_stats_for_season = get_bat_stats_for_season(season_id, bat_stats)
        return create_bat_stats_metrics(team_id=self.team_id_bbref, bat_stats=bat_stats_for_season, year=year)

    def _get_bat_stat_metrics_for_lineup_spots(
        self, bat_stats: List[db.BatStats], bat_order_list: List[int], season_id: int, year: int
    ):
        bat_stats_for_lineup_spots_for_season = get_bat_stats_for_lineup_spots(
            bat_order_list, get_bat_stats_for_season(season_id, bat_stats)
        )
        bat_stats_metrics = create_bat_stats_metrics(
            team_id=self.team_id_bbref,
            bat_stats=bat_stats_for_lineup_spots_for_season,
            year=year,
            bat_order_list=bat_order_list,
        )
        bat_stats_metrics.all_team_stats_for_bat_order = True
        return bat_stats_metrics

    def _get_bat_stat_metrics_for_def_positions(
        self, bat_stats: List[db.BatStats], def_position_list: List[DefensePosition], season_id: int, year: int
    ):
        bat_stats_for_def_positions_for_season = get_bat_stats_for_def_positions(
            def_position_list, get_bat_stats_for_season(season_id, bat_stats)
        )
        bat_stats_metrics = create_bat_stats_metrics(
            team_id=self.team_id_bbref,
            bat_stats=bat_stats_for_def_positions_for_season,
            year=year,
            def_position_list=def_position_list,
        )
        bat_stats_metrics.all_team_stats_for_def_pos = True
        return bat_stats_metrics

    def _get_bat_stat_metrics_for_starters(
        self, bat_stats: List[db.BatStats], season_id: int, year: int
    ) -> BatStatsMetrics:
        bat_stats_for_starters_for_season = get_bat_stats_for_starters(get_bat_stats_for_season(season_id, bat_stats))
        return create_bat_stats_metrics(
            team_id=self.team_id_bbref, bat_stats=bat_stats_for_starters_for_season, year=year, is_starter=True
        )

    def _get_bat_stat_metrics_for_bench(
        self, bat_stats: List[db.BatStats], season_id: int, year: int
    ) -> BatStatsMetrics:
        bat_stats_for_bench_for_season = get_bat_stats_for_bench(get_bat_stats_for_season(season_id, bat_stats))
        return create_bat_stats_metrics(
            team_id=self.team_id_bbref, bat_stats=bat_stats_for_bench_for_season, year=year, is_starter=False
        )

    def _get_bat_stat_metrics_for_player(
        self, bat_stats: List[db.BatStats], player_id: int, mlb_id: int, bbref_id: str, year: int, season_id: int
    ) -> BatStatsMetrics:
        bat_stats_for_player = get_bat_stats_for_player(player_id, get_bat_stats_for_season(season_id, bat_stats))
        return create_bat_stats_metrics(
            team_id=self.team_id_bbref,
            bat_stats=bat_stats_for_player,
            year=year,
            player_id_mlb=mlb_id,
            player_id_bbref=bbref_id,
        )

    def _get_bat_stat_metrics_when_starter_for_player(
        self, bat_stats: List[db.BatStats], player_id: int, mlb_id: int, bbref_id: str, year: int, season_id: int
    ) -> BatStatsMetrics:
        bat_stats_for_player = get_bat_stats_for_starters(
            get_bat_stats_for_player(
                player_id,
                get_bat_stats_for_season(
                    season_id,
                    bat_stats,
                ),
            ),
        )
        return create_bat_stats_metrics(
            team_id=self.team_id_bbref,
            bat_stats=bat_stats_for_player,
            year=year,
            player_id_mlb=mlb_id,
            player_id_bbref=bbref_id,
            is_starter=True,
        )

    def _get_bat_stat_metrics_when_bench_for_player(
        self, bat_stats: List[db.BatStats], player_id: int, mlb_id: int, bbref_id: str, year: int, season_id: int
    ) -> BatStatsMetrics:
        bat_stats_for_player = get_bat_stats_for_bench(
            get_bat_stats_for_player(
                player_id,
                get_bat_stats_for_season(
                    season_id,
                    bat_stats,
                ),
            ),
        )
        return create_bat_stats_metrics(
            team_id=self.team_id_bbref,
            bat_stats=bat_stats_for_player,
            year=year,
            player_id_mlb=mlb_id,
            player_id_bbref=bbref_id,
            is_starter=False,
        )

    def get_bat_stat_metrics_for_lineup_spots_for_player(
        self,
        bat_stats: List[db.BatStats],
        bat_order_list: List[int],
        player_id: int,
        mlb_id: int,
        bbref_id: str,
        year: int,
        season_id: int,
    ) -> BatStatsMetrics:
        bat_stats_for_lineup_spots_for_player = get_bat_stats_for_lineup_spots(
            bat_order_list,
            get_bat_stats_for_player(
                player_id,
                get_bat_stats_for_season(
                    season_id,
                    bat_stats,
                ),
            ),
        )
        return create_bat_stats_metrics(
            team_id=self.team_id_bbref,
            bat_stats=bat_stats_for_lineup_spots_for_player,
            year=year,
            player_id_mlb=mlb_id,
            player_id_bbref=bbref_id,
            bat_order_list=bat_order_list,
        )

    def get_bat_stat_metrics_for_def_positions_for_player(
        self,
        bat_stats: List[db.BatStats],
        def_position_list: List[DefensePosition],
        player_id: int,
        mlb_id: int,
        bbref_id: str,
        year: int,
        season_id: int,
    ) -> BatStatsMetrics:
        bat_stats_for_def_positions_for_player = get_bat_stats_for_def_positions(
            def_position_list,
            get_bat_stats_for_player(
                player_id,
                get_bat_stats_for_season(
                    season_id,
                    bat_stats,
                ),
            ),
        )
        return create_bat_stats_metrics(
            team_id=self.team_id_bbref,
            bat_stats=bat_stats_for_def_positions_for_player,
            year=year,
            player_id_mlb=mlb_id,
            player_id_bbref=bbref_id,
            def_position_list=def_position_list,
        )


def set_flag_for_separate_bat_order_metrics(bat_stats_metrics: BatStatsMetrics) -> BatStatsMetrics:
    bat_stats_metrics.separate_player_stats_for_bat_order = True
    return bat_stats_metrics


def set_flag_for_separate_def_pos_metrics(bat_stats_metrics: BatStatsMetrics) -> BatStatsMetrics:
    bat_stats_metrics.separate_player_stats_for_def_pos = True
    return bat_stats_metrics


def set_flag_for_combined_bat_order_metrics(bat_stats_metrics: BatStatsMetrics) -> BatStatsMetrics:
    bat_stats_metrics.all_player_stats_for_bat_order = True
    return bat_stats_metrics


def set_flag_for_combined_def_pos_metrics(bat_stats_metrics: BatStatsMetrics) -> BatStatsMetrics:
    bat_stats_metrics.all_player_stats_for_def_pos = True
    return bat_stats_metrics


def get_bat_stats_for_season(season_id: int, bat_stats: List[db.BatStats]) -> List[db.BatStats]:
    bat_stats_for_season = filter(lambda x: x.season_id == season_id, bat_stats)
    return list(bat_stats_for_season)


def get_bat_stats_for_player(player_id: int, bat_stats: List[db.BatStats]) -> List[db.BatStats]:
    bat_stats_for_player = filter(lambda x: x.player_id == player_id, bat_stats)
    return list(bat_stats_for_player)


def get_bat_stats_for_starters(bat_stats: List[db.BatStats]) -> List[db.BatStats]:
    bat_stats_for_staters = filter(lambda x: x.is_starter == 1, bat_stats)
    return list(bat_stats_for_staters)


def get_bat_stats_for_bench(bat_stats: List[db.BatStats]) -> List[db.BatStats]:
    bat_stats_for_bench = filter(lambda x: x.is_starter == 0, bat_stats)
    return list(bat_stats_for_bench)


def get_bat_stats_for_lineup_spots(bat_order_list: List[int], bat_stats: List[db.BatStats]) -> List[db.BatStats]:
    bat_stats_for_lineup_spots = filter(lambda x: x.bat_order in bat_order_list, bat_stats)
    return list(bat_stats_for_lineup_spots)


def get_bat_stats_for_def_positions(
    def_position_list: List[DefensePosition], bat_stats: List[db.BatStats]
) -> List[db.BatStats]:
    bat_stats_for_def_positions = filter(
        lambda x: x.def_position in convert_def_position_list_to_str_list(def_position_list), bat_stats
    )
    return list(bat_stats_for_def_positions)


def convert_def_position_list_to_str_list(def_positions: List[DefensePosition]) -> List[str]:
    return [str(int(def_pos)) for def_pos in def_positions]


def create_bat_stats_metrics(
    team_id: str,
    bat_stats: List[db.BatStats],
    year: int = None,
    player_id_mlb: int = None,
    player_id_bbref: str = None,
    is_starter: bool = False,
    bat_order_list: List[int] = 0,
    def_position_list: List[DefensePosition] = None,
) -> BatStatsMetrics:
    return BatStatsMetrics(
        bat_stats=deepcopy(bat_stats),
        year=year,
        player_id_mlb=player_id_mlb,
        player_id_bbref=player_id_bbref,
        team_id_bbref=team_id,
        player_team_id_bbref=team_id,
        is_starter=is_starter,
        bat_order_list=bat_order_list,
        def_position_list=def_position_list,
    )
