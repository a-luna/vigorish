from functools import cached_property
from typing import Dict, List, Tuple, Union

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

import vigorish.database as db
from vigorish.cli.components.viewers import create_display_table, create_table_viewer
from vigorish.data.metrics import BatStatsMetrics, PitchStatsMetrics
from vigorish.data.metrics.pfx_metrics_factory import (
    PfxMetricsFactory,
    PitchFxBattingMetrics,
    PitchFxPitchingMetrics,
    PitchFxMetricsSet,
)
from vigorish.data.scraped_data import ScrapedData
from vigorish.enums import PitchType
from vigorish.models.batter_percentiles import BatterPercentile
from vigorish.util.exceptions import UnknownPlayerException
from vigorish.util.string_helpers import validate_pitch_app_id

PitchTypePercentiles = List[Dict[str, Union[PitchType, Tuple[float, float]]]]


class PlayerData:
    def __init__(self, app, mlb_id: int):
        self.app = app
        self.mlb_id = mlb_id
        self.db_engine: Engine = self.app.db_engine
        self.db_session: Session = self.app.db_session
        self.scraped_data: ScrapedData = self.app.scraped_data
        self.pfx_metrics = PfxMetricsFactory(app)
        self.player = db.Player.find_by_mlb_id(self.db_session, self.mlb_id)
        if not self.player:
            raise UnknownPlayerException(mlb_id)
        self.player_id = db.PlayerId.find_by_mlb_id(self.db_session, self.mlb_id)
        self.pitchfx_game_pitch_data_cache = {}
        self.pitchfx_game_bat_data_cache = {}

    @property
    def player_name(self) -> str:
        return self.player_id.mlb_name

    @property
    def years_played(self) -> List[int]:
        return [s.year for s in self.seasons_played] if self.seasons_played else []

    @cached_property
    def all_teams_played_for(self) -> List[Dict[str, Union[bool, int, str]]]:
        team_assoc_list = (
            self.db_session.query(db.Assoc_Player_Team)
            .filter_by(db_player_id=self.player.id)
            .filter(db.Assoc_Player_Team.year >= 2017)
            .order_by(db.Assoc_Player_Team.year)
            .order_by(db.Assoc_Player_Team.stint_number)
            .all()
        )
        return [team.as_dict() for team in team_assoc_list]

    @property
    def player_details(self):
        player_details = self.player.as_dict()
        player_details.pop("id")
        player_details.pop("retro_id")
        player_details.pop("scraped_transactions")
        player_details.pop("minor_league_player")
        player_details.pop("missing_mlb_id")
        player_details.pop("add_to_db_backup")
        player_details["all_teams"] = self.all_teams_played_for
        return player_details

    @property
    def pitch_app_map(self) -> Dict[str, db.PitchAppScrapeStatus]:
        return {pitch_app.bbref_game_id: pitch_app for pitch_app in self.player.pitch_apps}

    @cached_property
    def seasons_played(self) -> List[db.Season]:
        return self.scraped_data.get_all_seasons_with_data_for_player(self.mlb_id)

    @cached_property
    def pitch_stats_for_career(self) -> PitchStatsMetrics:
        return self.scraped_data.get_pitch_stats_for_career_for_player(self.mlb_id)

    @cached_property
    def pitch_stats_as_sp(self) -> PitchStatsMetrics:
        return self.scraped_data.get_pitch_stats_as_sp_for_player(self.mlb_id)

    @cached_property
    def pitch_stats_as_rp(self) -> PitchStatsMetrics:
        return self.scraped_data.get_pitch_stats_as_rp_for_player(self.mlb_id)

    @cached_property
    def pitch_stats_by_year(self) -> List[PitchStatsMetrics]:
        return self.scraped_data.get_pitch_stats_by_year_for_player(self.mlb_id)

    @cached_property
    def pitch_stats_by_team(self) -> List[PitchStatsMetrics]:
        return self.scraped_data.get_pitch_stats_by_team_for_player(self.mlb_id)

    @cached_property
    def pitch_stats_by_team_by_year(self) -> List[PitchStatsMetrics]:
        return self.scraped_data.get_pitch_stats_by_team_by_year_for_player(self.mlb_id)

    @cached_property
    def pitch_stats_by_opp_team(self) -> List[PitchStatsMetrics]:
        return self.scraped_data.get_pitch_stats_by_opp_team_for_player(self.mlb_id)

    @cached_property
    def pitch_stats_by_opp_team_by_year(self) -> List[PitchStatsMetrics]:
        return self.scraped_data.get_pitch_stats_by_opp_team_by_year_for_player(self.mlb_id)

    @cached_property
    def bat_stats_for_career(self) -> BatStatsMetrics:
        return self.scraped_data.get_bat_stats_for_career_for_player(self.mlb_id)

    @cached_property
    def bat_stats_by_year(self) -> List[BatStatsMetrics]:
        return self.scraped_data.get_bat_stats_by_year_for_player(self.mlb_id)

    @cached_property
    def bat_stats_by_team(self) -> List[BatStatsMetrics]:
        return self.scraped_data.get_bat_stats_by_team_for_player(self.mlb_id)

    @cached_property
    def bat_stats_by_team_by_year(self) -> List[BatStatsMetrics]:
        return self.scraped_data.get_bat_stats_by_team_by_year_for_player(self.mlb_id)

    @cached_property
    def bat_stats_by_opp_team(self) -> List[BatStatsMetrics]:
        return self.scraped_data.get_bat_stats_by_opp_for_player(self.mlb_id)

    @cached_property
    def bat_stats_by_opp_team_by_year(self) -> List[BatStatsMetrics]:
        return self.scraped_data.get_bat_stats_by_opp_by_year_for_player(self.mlb_id)

    @cached_property
    def pfx_pitching_metrics_for_career(self) -> PitchFxPitchingMetrics:
        return self.pfx_metrics.for_pitcher_career(self.mlb_id, self.player.throws)

    @property
    def pfx_pitching_metrics_vs_all_for_career(self) -> PitchFxMetricsSet:
        return self.pfx_pitching_metrics_for_career.all

    @cached_property
    def percentiles_for_pitch_types_for_career(self) -> List[PitchTypePercentiles]:
        return [
            self.scraped_data.calculate_pitch_type_percentiles(self.player.throws, pfx_metrics)
            for pfx_metrics in self.pfx_pitching_metrics_vs_all_for_career.metrics_by_pitch_type.values()
        ]

    @property
    def pfx_pitching_metrics_vs_rhb_for_career(self) -> PitchFxMetricsSet:
        return self.pfx_pitching_metrics_for_career.rhb

    @cached_property
    def percentiles_for_pitch_types_vs_rhb_for_career(self) -> List[PitchTypePercentiles]:
        return [
            self.scraped_data.calculate_pitch_type_percentiles(self.player.throws, pfx_metrics)
            for pfx_metrics in self.pfx_pitching_metrics_vs_rhb_for_career.metrics_by_pitch_type.values()
        ]

    @property
    def pfx_pitching_metrics_vs_lhb_for_career(self) -> PitchFxMetricsSet:
        return self.pfx_pitching_metrics_for_career.lhb

    @cached_property
    def percentiles_for_pitch_types_vs_lhb_for_career(self) -> List[PitchTypePercentiles]:
        return [
            self.scraped_data.calculate_pitch_type_percentiles(self.player.throws, pfx_metrics)
            for pfx_metrics in self.pfx_pitching_metrics_vs_lhb_for_career.metrics_by_pitch_type.values()
        ]

    def get_all_pfx_career_data(
        self,
    ) -> Dict[str, Dict[str, Union[List[PitchTypePercentiles], PitchFxMetricsSet]]]:
        return {
            "all": {
                "metrics": self.pfx_pitching_metrics_vs_all_for_career,
                "percentiles": self.percentiles_for_pitch_types_for_career,
            },
            "rhb": {
                "metrics": self.pfx_pitching_metrics_vs_rhb_for_career,
                "percentiles": self.percentiles_for_pitch_types_vs_rhb_for_career,
            },
            "lhb": {
                "metrics": self.pfx_pitching_metrics_vs_lhb_for_career,
                "percentiles": self.percentiles_for_pitch_types_vs_lhb_for_career,
            },
        }

    @cached_property
    def pfx_pitching_metrics_by_year(self) -> Dict[int, PitchFxPitchingMetrics]:
        return self.pfx_metrics.for_pitcher_by_year(self.mlb_id, self.player.throws)

    @property
    def pfx_pitching_metrics_vs_all_by_year(self) -> Dict[int, PitchFxMetricsSet]:
        return {
            year: pfx_metrics_collection.all
            for year, pfx_metrics_collection in self.pfx_pitching_metrics_by_year.items()
        }

    @cached_property
    def percentiles_for_pitch_types_by_year(self) -> Dict[int, List[PitchTypePercentiles]]:
        return {
            year: [
                self.scraped_data.calculate_pitch_type_percentiles(self.player.throws, pfx_metrics)
                for pfx_metrics in pfx_metrics_collection.metrics_by_pitch_type.values()
            ]
            for year, pfx_metrics_collection in self.pfx_pitching_metrics_vs_all_by_year.items()
        }

    @property
    def pfx_pitching_metrics_vs_rhb_by_year(self) -> Dict[int, PitchFxMetricsSet]:
        return {
            year: pfx_metrics_collection.rhb
            for year, pfx_metrics_collection in self.pfx_pitching_metrics_by_year.items()
        }

    @cached_property
    def percentiles_for_pitch_types_vs_rhb_by_year(
        self,
    ) -> Dict[int, List[PitchTypePercentiles]]:
        return {
            year: [
                self.scraped_data.calculate_pitch_type_percentiles(self.player.throws, pfx_metrics)
                for pfx_metrics in pfx_metrics_collection.metrics_by_pitch_type.values()
            ]
            for year, pfx_metrics_collection in self.pfx_pitching_metrics_vs_rhb_by_year.items()
        }

    @property
    def pfx_pitching_metrics_vs_lhb_by_year(self) -> Dict[int, PitchFxMetricsSet]:
        return {
            year: pfx_metrics_collection.lhb
            for year, pfx_metrics_collection in self.pfx_pitching_metrics_by_year.items()
        }

    @cached_property
    def percentiles_for_pitch_types_vs_lhb_by_year(
        self,
    ) -> Dict[int, List[PitchTypePercentiles]]:
        return {
            year: [
                self.scraped_data.calculate_pitch_type_percentiles(self.player.throws, pfx_metrics)
                for pfx_metrics in pfx_metrics_collection.metrics_by_pitch_type.values()
            ]
            for year, pfx_metrics_collection in self.pfx_pitching_metrics_vs_lhb_by_year.items()
        }

    def get_all_pfx_yearly_data(
        self,
    ) -> Dict[str, Dict[str, Dict[int, Union[PitchFxMetricsSet, PitchTypePercentiles]]]]:
        return {
            "all": {
                "metrics": self.pfx_pitching_metrics_vs_all_by_year,
                "percentiles": self.percentiles_for_pitch_types_by_year,
            },
            "rhb": {
                "metrics": self.pfx_pitching_metrics_vs_rhb_by_year,
                "percentiles": self.percentiles_for_pitch_types_vs_rhb_by_year,
            },
            "lhb": {
                "metrics": self.pfx_pitching_metrics_vs_lhb_by_year,
                "percentiles": self.percentiles_for_pitch_types_vs_lhb_by_year,
            },
        }

    @cached_property
    def pfx_batting_metrics_for_career(self) -> PitchFxBattingMetrics:
        return self.pfx_metrics.for_batter_career(self.mlb_id)

    @property
    def pfx_batting_metrics_vs_all_for_career(self) -> PitchFxMetricsSet:
        return self.pfx_batting_metrics_for_career.vs_all

    @cached_property
    def bat_stat_percentiles_vs_all_for_career(self) -> List[BatterPercentile]:
        return self.scraped_data.calculate_batter_percentiles(
            self.pfx_batting_metrics_vs_all_for_career.metrics_combined
        )

    @property
    def pfx_batting_metrics_vs_rhp_for_career(self) -> PitchFxMetricsSet:
        return self.pfx_batting_metrics_for_career.vs_rhp

    @cached_property
    def bat_stat_percentiles_vs_rhp_career(self) -> List[BatterPercentile]:
        return self.scraped_data.calculate_batter_percentiles(
            self.pfx_batting_metrics_vs_rhp_for_career.metrics_combined
        )

    @property
    def pfx_batting_metrics_vs_lhp_for_career(self) -> PitchFxMetricsSet:
        return self.pfx_batting_metrics_for_career.vs_lhp

    @cached_property
    def bat_stat_percentiles_vs_lhp_career(self) -> List[BatterPercentile]:
        return self.scraped_data.calculate_batter_percentiles(
            self.pfx_batting_metrics_vs_lhp_for_career.metrics_combined
        )

    @property
    def pfx_batting_metrics_vs_rhp_as_rhb_for_career(self) -> PitchFxMetricsSet:
        return self.pfx_batting_metrics_for_career.as_rhb_vs_rhp

    @property
    def pfx_batting_metrics_vs_rhp_as_lhb_for_career(self) -> PitchFxMetricsSet:
        return self.pfx_batting_metrics_for_career.as_lhb_vs_rhp

    @property
    def pfx_batting_metrics_vs_lhp_as_lhb_for_career(self) -> PitchFxMetricsSet:
        return self.pfx_batting_metrics_for_career.as_lhb_vs_lhp

    @property
    def pfx_batting_metrics_vs_lhp_as_rhb_for_career(self) -> PitchFxMetricsSet:
        return self.pfx_batting_metrics_for_career.as_rhb_vs_lhp

    def get_all_pfx_batting_career_data(
        self,
    ) -> Dict[str, Dict[str, Union[List[PitchTypePercentiles], PitchFxMetricsSet]]]:
        return {
            "vs_all": {
                "metrics": self.pfx_batting_metrics_vs_all_for_career,
                "percentiles": self.bat_stat_percentiles_vs_all_for_career,
            },
            "vs_rhp": {
                "metrics": self.pfx_batting_metrics_vs_rhp_for_career,
                "percentiles": self.bat_stat_percentiles_vs_rhp_career,
            },
            "vs_lhp": {
                "metrics": self.pfx_batting_metrics_vs_lhp_for_career,
                "percentiles": self.bat_stat_percentiles_vs_lhp_career,
            },
        }

    @cached_property
    def pfx_batting_metrics_by_year(self) -> Dict[int, PitchFxBattingMetrics]:
        return self.pfx_metrics.for_batter_by_year(self.mlb_id)

    @property
    def pfx_batting_metrics_vs_all_by_year(self) -> Dict[int, PitchFxMetricsSet]:
        return {
            year: pfx_metrics_collection.vs_all
            for year, pfx_metrics_collection in self.pfx_batting_metrics_by_year.items()
        }

    @cached_property
    def bat_stat_percentiles_vs_all_by_year(self) -> Dict[int, BatterPercentile]:
        return {
            year: [self.scraped_data.calculate_batter_percentiles(pfx_metrics_collection.metrics_combined)]
            for year, pfx_metrics_collection in self.pfx_batting_metrics_vs_all_by_year.items()
        }

    @property
    def pfx_batting_metrics_vs_rhp_by_year(self) -> Dict[int, PitchFxMetricsSet]:
        return {
            year: pfx_metrics_collection.vs_rhp
            for year, pfx_metrics_collection in self.pfx_batting_metrics_by_year.items()
        }

    @cached_property
    def bat_stat_percentiles_vs_rhp_by_year(self) -> Dict[int, BatterPercentile]:
        return {
            year: [self.scraped_data.calculate_batter_percentiles(pfx_metrics_collection.metrics_combined)]
            for year, pfx_metrics_collection in self.pfx_batting_metrics_vs_rhp_by_year.items()
        }

    @property
    def pfx_batting_metrics_vs_rhp_as_rhb_by_year(self) -> Dict[int, PitchFxMetricsSet]:
        return {
            year: pfx_metrics_collection.as_rhb_vs_rhp
            for year, pfx_metrics_collection in self.pfx_batting_metrics_by_year.items()
        }

    @property
    def pfx_batting_metrics_vs_rhp_as_lhb_by_year(self) -> Dict[int, PitchFxMetricsSet]:
        return {
            year: pfx_metrics_collection.as_lhb_vs_rhp
            for year, pfx_metrics_collection in self.pfx_batting_metrics_by_year.items()
        }

    @property
    def pfx_batting_metrics_vs_lhp_by_year(self) -> Dict[int, PitchFxMetricsSet]:
        return {
            year: pfx_metrics_collection.vs_lhp
            for year, pfx_metrics_collection in self.pfx_batting_metrics_by_year.items()
        }

    @cached_property
    def bat_stat_percentiles_vs_lhp_by_year(self) -> Dict[int, BatterPercentile]:
        return {
            year: [self.scraped_data.calculate_batter_percentiles(pfx_metrics_collection.metrics_combined)]
            for year, pfx_metrics_collection in self.pfx_batting_metrics_vs_lhp_by_year.items()
        }

    @property
    def pfx_batting_metrics_vs_lhp_as_rhb_by_year(self) -> Dict[int, PitchFxMetricsSet]:
        return {
            year: pfx_metrics_collection.as_rhb_vs_lhp
            for year, pfx_metrics_collection in self.pfx_batting_metrics_by_year.items()
        }

    @property
    def pfx_batting_metrics_vs_lhp_as_lhb_by_year(self) -> Dict[int, PitchFxMetricsSet]:
        return {
            year: pfx_metrics_collection.as_lhb_vs_lhp
            for year, pfx_metrics_collection in self.pfx_batting_metrics_by_year.items()
        }

    def get_all_pfx_batting_yearly_data(
        self,
    ) -> Dict[str, Dict[str, Dict[int, Dict[str, Union[List[BatterPercentile], PitchFxMetricsSet]]]]]:
        return {
            "all": {
                "metrics": self.pfx_batting_metrics_vs_all_by_year,
                "percentiles": self.bat_stat_percentiles_vs_all_by_year,
            },
            "rhb": {
                "metrics": self.pfx_batting_metrics_vs_rhp_by_year,
                "percentiles": self.bat_stat_percentiles_vs_rhp_by_year,
            },
            "lhb": {
                "metrics": self.pfx_batting_metrics_vs_lhp_by_year,
                "percentiles": self.bat_stat_percentiles_vs_lhp_by_year,
            },
        }

    def get_pfx_pitching_metrics_vs_all_for_season(self, year: int) -> PitchFxMetricsSet:
        pfx_metrics_for_season = self.pfx_metrics.for_pitcher_season(self.mlb_id, year, self.player.throws)
        return pfx_metrics_for_season.all

    def get_pfx_pitching_metrics_vs_rhb_for_season(self, year: int) -> PitchFxMetricsSet:
        pfx_metrics_for_season = self.pfx_metrics.for_pitcher_season(self.mlb_id, year, self.player.throws)
        return pfx_metrics_for_season.rhb

    def get_pfx_pitching_metrics_vs_lhb_for_season(self, year: int) -> PitchFxMetricsSet:
        pfx_metrics_for_season = self.pfx_metrics.for_pitcher_season(self.mlb_id, year, self.player.throws)
        return pfx_metrics_for_season.lhb

    def get_pfx_pitching_metrics_vs_all_for_game(self, bbref_game_id: str) -> PitchFxMetricsSet:
        return self.get_pfx_pitch_metrics_for_game(bbref_game_id=bbref_game_id).all

    def get_pfx_pitching_metrics_vs_rhb_for_game(self, bbref_game_id: str) -> PitchFxMetricsSet:
        return self.get_pfx_pitch_metrics_for_game(bbref_game_id=bbref_game_id).rhb

    def get_pfx_pitching_metrics_vs_lhb_for_game(self, bbref_game_id: str) -> PitchFxMetricsSet:
        return self.get_pfx_pitch_metrics_for_game(bbref_game_id=bbref_game_id).lhb

    def get_pfx_batting_metrics_vs_all_for_game(self, bbref_game_id) -> PitchFxMetricsSet:
        return self.get_pfx_bat_metrics_for_game(bbref_game_id).vs_all

    def get_pfx_batting_metrics_vs_rhp_for_game(self, bbref_game_id) -> PitchFxMetricsSet:
        return self.get_pfx_bat_metrics_for_game(bbref_game_id).vs_rhp

    def get_pfx_batting_metrics_vs_lhp_for_game(self, bbref_game_id) -> PitchFxMetricsSet:
        return self.get_pfx_bat_metrics_for_game(bbref_game_id).vs_lhp

    def get_pfx_batting_metrics_vs_rhp_as_rhb_for_game(self, bbref_game_id) -> PitchFxMetricsSet:
        return self.get_pfx_bat_metrics_for_game(bbref_game_id).as_rhb_vs_rhp

    def get_pfx_batting_metrics_vs_rhp_as_lhb_for_game(self, bbref_game_id) -> PitchFxMetricsSet:
        return self.get_pfx_bat_metrics_for_game(bbref_game_id).as_lhb_vs_rhp

    def get_pfx_batting_metrics_vs_lhp_as_lhb_for_game(self, bbref_game_id) -> PitchFxMetricsSet:
        return self.get_pfx_bat_metrics_for_game(bbref_game_id).as_lhb_vs_lhp

    def get_pfx_batting_metrics_vs_lhp_as_rhb_for_game(self, bbref_game_id) -> PitchFxMetricsSet:
        return self.get_pfx_bat_metrics_for_game(bbref_game_id).as_rhb_vs_lhp

    def get_pfx_bat_metrics_for_game(self, bbref_game_id):
        pfx_metrics_for_game = self.pitchfx_game_bat_data_cache.get(bbref_game_id)
        if not pfx_metrics_for_game:
            pfx_metrics_for_game = self.pfx_metrics.for_batter_game(self.mlb_id, bbref_game_id)
            self.pitchfx_game_bat_data_cache[bbref_game_id] = pfx_metrics_for_game
        return pfx_metrics_for_game

    def get_pfx_pitch_metrics_for_game(self, bbref_game_id=None, pitch_app_id=None):
        (is_valid, bbref_game_id, pitch_app_id) = self.validate_pitch_app(bbref_game_id, pitch_app_id)
        if not is_valid:
            return None
        pfx_metrics_for_game = self.pitchfx_game_pitch_data_cache.get(bbref_game_id)
        if not pfx_metrics_for_game:
            pfx_metrics_for_game = self.pfx_metrics.for_pitcher_game(self.mlb_id, bbref_game_id, self.player.throws)
            self.pitchfx_game_pitch_data_cache[bbref_game_id] = pfx_metrics_for_game
        return pfx_metrics_for_game

    def validate_pitch_app(self, bbref_game_id=None, pitch_app_id=None):
        is_valid = False
        if not pitch_app_id and not bbref_game_id:
            return (is_valid, bbref_game_id, pitch_app_id)
        if bbref_game_id:
            is_valid = bbref_game_id in self.pitch_app_map
        else:
            try:
                bbref_game_id = validate_pitch_app_id(pitch_app_id).value["game_id"]
            except AttributeError:
                return (is_valid, bbref_game_id, pitch_app_id)
        if not pitch_app_id:
            pitch_app_id = f"{bbref_game_id}_{self.mlb_id}"
        return (is_valid, bbref_game_id, pitch_app_id)

    def view_pitch_mix_batter_stance_splits(self, game_id):
        return create_table_viewer(
            [
                create_display_table(
                    table_rows=self.get_pitch_mix_batter_stance_splits_for_game(game_id),
                    heading=f"Pitch Mix for {self.player_name} by Batter Stance (This Game)",
                    table_headers="keys",
                ),
                create_display_table(
                    table_rows=self.get_pitch_mix_batter_stance_splits_for_career(),
                    heading=f"Pitch Mix for {self.player_name} by Batter Stance (Career)",
                    table_headers="keys",
                ),
            ]
        )

    def get_pitch_mix_batter_stance_splits_for_game(self, game_id):
        pitch_metrics = self.get_pfx_pitch_metrics_for_game(bbref_game_id=game_id)
        return self.get_pitch_mix_batter_stance_splits(
            pitch_metrics.all, pitch_metrics.rhb, pitch_metrics.lhb, include_pitch_count=True
        )

    def get_pitch_mix_batter_stance_splits_for_career(self):
        return self.get_pitch_mix_batter_stance_splits(
            self.pfx_pitching_metrics_vs_all_for_career,
            self.pfx_pitching_metrics_vs_rhb_for_career,
            self.pfx_pitching_metrics_vs_lhb_for_career,
        )

    def get_pitch_mix_batter_stance_splits(
        self, pitch_metrics_all, pitch_metrics_vs_rhb, pitch_metrics_vs_lhb, include_pitch_count=False
    ):
        return [
            {
                "pitch_type": pitch_type.print_name,
                "all": pitch_metrics_all.get_usage_stats_for_pitch_type(pitch_type, include_pitch_count),
                "vs_RHB": pitch_metrics_vs_rhb.get_usage_stats_for_pitch_type(pitch_type, include_pitch_count),
                "vs_LHB": pitch_metrics_vs_lhb.get_usage_stats_for_pitch_type(pitch_type, include_pitch_count),
            }
            for pitch_type in pitch_metrics_all.pitch_type
        ]

    def view_pitch_mix_season_splits(self, game_id):
        return create_table_viewer(
            [
                create_display_table(
                    table_rows=self.get_pitch_mix_season_splits(game_id),
                    heading=f"Pitch Mix for {self.player_name} by Season",
                    table_headers="keys",
                )
            ]
        )

    def get_pitch_mix_season_splits(self, game_id):
        return [
            self.get_pitch_mix_season_splits_for_pitch_type(game_id, pitch_type)
            for pitch_type in self.pfx_pitching_metrics_vs_all_for_career.pitch_type
        ]

    def get_pitch_mix_season_splits_for_pitch_type(self, game_id, pitch_type):
        pitch_metrics_for_game = self.get_pfx_pitch_metrics_for_game(bbref_game_id=game_id)
        table_row = {
            "pitch_type": pitch_type.print_name,
            "game": pitch_metrics_for_game.all.get_usage_stats_for_pitch_type(pitch_type),
            "all": self.pfx_pitching_metrics_vs_all_for_career.get_usage_stats_for_pitch_type(pitch_type),
        }
        for year, pitch_metrics_for_year in self.pfx_pitching_metrics_vs_all_by_year.items():
            table_row[str(year)] = pitch_metrics_for_year.get_usage_stats_for_pitch_type(pitch_type)
        return table_row

    def view_plate_discipline_pitch_type_splits(self, game_id):
        return create_table_viewer(
            [
                create_display_table(
                    table_rows=self.get_plate_discipline_pitch_type_splits_for_game(game_id),
                    heading=f"Plate Discipline Stats for {self.player_name} (This Game)",
                    table_headers="keys",
                ),
                create_display_table(
                    table_rows=self.get_plate_discipline_pitch_type_splits_for_career(),
                    heading=f"Plate Discipline Stats for {self.player_name} (Career)",
                    table_headers="keys",
                ),
            ]
        )

    def get_plate_discipline_pitch_type_splits_for_game(self, game_id):
        pitch_metrics = self.get_pfx_pitch_metrics_for_game(bbref_game_id=game_id)
        return pitch_metrics.all.get_plate_discipline_pitch_type_splits(include_pitch_count=True)

    def get_plate_discipline_pitch_type_splits_for_career(self):
        return self.pfx_pitching_metrics_vs_all_for_career.get_plate_discipline_pitch_type_splits()

    def view_batted_ball_pitch_type_splits(self, game_id):
        return create_table_viewer(
            [
                create_display_table(
                    table_rows=self.get_batted_ball_pitch_type_splits_for_game(game_id),
                    heading=f"Batted Ball Stats for {self.player_name} (This Game)",
                    table_headers="keys",
                ),
                create_display_table(
                    table_rows=self.get_batted_ball_pitch_type_splits_for_career(),
                    heading=f"Batted Ball Stats for {self.player_name} (Career)",
                    table_headers="keys",
                ),
            ]
        )

    def get_batted_ball_pitch_type_splits_for_game(self, game_id):
        pitch_metrics = self.get_pfx_pitch_metrics_for_game(bbref_game_id=game_id)
        return pitch_metrics.all.get_batted_ball_pitch_type_splits(include_bip_count=True)

    def get_batted_ball_pitch_type_splits_for_career(self):
        return self.pfx_pitching_metrics_vs_all_for_career.get_batted_ball_pitch_type_splits()

    def view_bat_stats_pitch_type_splits(self, game_id):
        return create_table_viewer(
            [
                create_display_table(
                    table_rows=self.get_bat_stats_pitch_type_splits_for_game(game_id),
                    heading=f"Batting Stats for {self.player_name} (This Game)",
                    table_headers="keys",
                ),
                create_display_table(
                    table_rows=self.get_bat_stats_pitch_type_splits_for_career(),
                    heading=f"Batting Stats for {self.player_name} (Career)",
                    table_headers="keys",
                ),
            ]
        )

    def get_bat_stats_pitch_type_splits_for_game(self, game_id):
        pitch_metrics = self.get_pfx_pitch_metrics_for_game(bbref_game_id=game_id)
        return pitch_metrics.all.get_bat_stats_pitch_type_splits(include_pa_count=True)

    def get_bat_stats_pitch_type_splits_for_career(self):
        return self.pfx_pitching_metrics_vs_all_for_career.get_bat_stats_pitch_type_splits()
