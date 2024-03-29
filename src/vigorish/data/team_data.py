from functools import cached_property

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

import vigorish.database as db
from vigorish.app import Vigorish
from vigorish.data.metrics import BatStatsMetrics, PitchStatsMetrics
from vigorish.data.scraped_data import ScrapedData
from vigorish.enums import DefensePosition


class TeamData:
    def __init__(self, app: Vigorish, team_id_bbref: str, year: int):
        self.app = app
        self.team_id_bbref = team_id_bbref
        self.year = year
        self.db_engine: Engine = self.app.db_engine
        self.db_session: Session = self.app.db_session
        self.scraped_data: ScrapedData = self.app.scraped_data
        self.team: db.Team = db.Team.find_by_team_id_and_year(self.db_session, self.team_id_bbref, self.year)

    @cached_property
    def pitch_stats(self) -> PitchStatsMetrics:
        return self.scraped_data.get_pitch_stats_for_team(self.team_id_bbref, self.year)

    @cached_property
    def pitch_stats_by_year(self) -> dict[int, PitchStatsMetrics]:
        return self.scraped_data.get_pitch_stats_by_year_for_team(self.team_id_bbref)

    @cached_property
    def pitch_stats_by_player(self) -> list[PitchStatsMetrics]:
        return self.scraped_data.get_pitch_stats_by_player_for_team(self.team_id_bbref, self.year)

    @cached_property
    def pitch_stats_for_sp(self) -> PitchStatsMetrics:
        return self.scraped_data.get_pitch_stats_for_sp_for_team(self.team_id_bbref, self.year)

    @cached_property
    def pitch_stats_for_sp_by_year(self) -> dict[int, PitchStatsMetrics]:
        return self.scraped_data.get_pitch_stats_for_sp_by_year_for_team(self.team_id_bbref)

    @cached_property
    def pitch_stats_for_sp_by_player(self) -> list[PitchStatsMetrics]:
        return self.scraped_data.get_pitch_stats_for_sp_by_player_for_team(self.team_id_bbref, self.year)

    @cached_property
    def pitch_stats_for_rp(self) -> PitchStatsMetrics:
        return self.scraped_data.get_pitch_stats_for_rp_for_team(self.team_id_bbref, self.year)

    @cached_property
    def pitch_stats_for_rp_by_year(self) -> dict[int, PitchStatsMetrics]:
        return self.scraped_data.get_pitch_stats_for_rp_by_year_for_team(self.team_id_bbref)

    @cached_property
    def pitch_stats_for_rp_by_player(self) -> list[PitchStatsMetrics]:
        return self.scraped_data.get_pitch_stats_for_rp_by_player_for_team(self.team_id_bbref, self.year)

    @cached_property
    def bat_stats(self) -> BatStatsMetrics:
        return self.scraped_data.get_bat_stats_for_team(self.team_id_bbref, self.year)

    @cached_property
    def bat_stats_by_year(self) -> dict[int, BatStatsMetrics]:
        return self.scraped_data.get_bat_stats_by_year_for_team(self.team_id_bbref)

    @cached_property
    def bat_stats_by_player(self) -> list[BatStatsMetrics]:
        return self.scraped_data.get_bat_stats_by_player_for_team(self.team_id_bbref, self.year)

    @cached_property
    def bat_stats_by_lineup_spot(self) -> list[BatStatsMetrics]:
        return self.scraped_data.get_bat_stats_by_lineup_spot_for_team(self.team_id_bbref, self.year)

    @cached_property
    def bat_stats_by_defpos(self) -> list[BatStatsMetrics]:
        return self.scraped_data.get_bat_stats_by_defpos_for_team(self.team_id_bbref, self.year)

    @cached_property
    def bat_stats_for_starters(self) -> BatStatsMetrics:
        return self.scraped_data.get_bat_stats_for_starters_for_team(self.team_id_bbref, self.year)

    @cached_property
    def bat_stats_for_starters_by_year(self) -> dict[int, BatStatsMetrics]:
        return self.scraped_data.get_bat_stats_for_starters_by_year_for_team(self.team_id_bbref)

    @cached_property
    def bat_stats_for_starters_by_player(self) -> list[BatStatsMetrics]:
        return self.scraped_data.get_bat_stats_for_starters_by_player_for_team(self.team_id_bbref, self.year)

    @cached_property
    def bat_stats_for_subs(self) -> BatStatsMetrics:
        return self.scraped_data.get_bat_stats_for_subs_for_team(self.team_id_bbref, self.year)

    @cached_property
    def bat_stats_for_subs_by_year(self) -> dict[int, BatStatsMetrics]:
        return self.scraped_data.get_bat_stats_for_subs_by_year_for_team(self.team_id_bbref)

    @cached_property
    def bat_stats_for_subs_by_player(self) -> list[BatStatsMetrics]:
        return self.scraped_data.get_bat_stats_for_subs_by_player_for_team(self.team_id_bbref, self.year)

    def get_bat_stats_for_lineup_spot_by_year(self, bat_order_list: list[int]) -> dict[int, BatStatsMetrics]:
        return self.scraped_data.get_bat_stats_for_lineup_spot_by_year_for_team(bat_order_list, self.team_id_bbref)

    def get_bat_stats_for_lineup_spot_by_player(self, bat_order_list: list[int]) -> list[BatStatsMetrics]:
        return self.scraped_data.get_bat_stats_for_lineup_spot_by_player_for_team(
            bat_order_list, self.team_id_bbref, self.year
        )

    def get_bat_stats_for_defpos_by_year(self, def_position_list: list[DefensePosition]) -> dict[int, BatStatsMetrics]:
        return self.scraped_data.get_bat_stats_for_defpos_by_year_for_team(def_position_list, self.team_id_bbref)

    def get_bat_stats_for_defpos_by_player(self, def_position_list: list[DefensePosition]) -> list[BatStatsMetrics]:
        return self.scraped_data.get_bat_stats_for_defpos_by_player_for_team(
            def_position_list, self.team_id_bbref, self.year
        )
