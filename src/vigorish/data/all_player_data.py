from functools import cached_property

from sqlalchemy import select

from vigorish.cli.components.viewers import create_display_table, create_table_viewer
from vigorish.database import (  # Player_BatStats_All_View,; Player_BatStats_By_Opp_Team_View,; Player_BatStats_By_Opp_Team_Year_View,; Player_BatStats_By_Team_View,; Player_BatStats_By_Team_Year_View,; Player_BatStats_By_Year_View,; Player_PitchStats_All_View,; Player_PitchStats_By_Opp_Team_View,; Player_PitchStats_By_Opp_Team_Year_View,; Player_PitchStats_By_Team_View,; Player_PitchStats_By_Team_Year_View,; Player_PitchStats_By_Year_View,; Player_PitchStats_RP_View,; Player_PitchStats_SP_View,
    Pitch_Type_All_View,
    Pitch_Type_By_Year_View,
    Pitch_Type_Left_View,
    Pitch_Type_Right_View,
    PitchApp_PitchType_All_View,
    PitchApp_PitchType_Left_View,
    PitchApp_PitchType_Right_View,
    Player,
    PlayerId,
    Season,
)
from vigorish.util.string_helpers import validate_pitch_app_id


class AllPlayerData:
    def __init__(self, app, mlb_id):
        self.app = app
        self.db_engine = self.app.db_engine
        self.db_session = self.app.db_session
        self.mlb_id = mlb_id
        self.player = Player.find_by_mlb_id(self.db_session, self.mlb_id)
        self.player_id = PlayerId.find_by_mlb_id(self.db_session, self.mlb_id)
        self.pitch_app_dict = {}

    @property
    def player_name(self):
        return self.player_id.mlb_name

    @property
    def years_played(self):
        return [s.year for s in self.seasons_played] if self.seasons_played else []

    @property
    def pitch_app_map(self):
        return {pitch_app.bbref_game_id: pitch_app for pitch_app in self.player.pitch_apps}

    @cached_property
    def seasons_played(self):
        s = select([Pitch_Type_By_Year_View.season_id]).where(Pitch_Type_By_Year_View.id == self.player.id).distinct()
        results = self.db_engine.execute(s).fetchall()
        seasons_played = []
        for d in [dict(row) for row in results]:
            season = self.db_session.query(Season).get(d["season_id"])
            seasons_played.append(season)
        return sorted(seasons_played, key=lambda x: x.year) if seasons_played else []

    @cached_property
    def pitchfx_metrics_career(self):
        return Pitch_Type_All_View.get_pitchfx_metrics_for_career_for_player(self.db_engine, self.player.id)

    @cached_property
    def pitchfx_metrics_vs_rhb(self):
        return Pitch_Type_Right_View.get_pitchfx_metrics_vs_rhb_for_player(self.db_engine, self.player.id)

    @cached_property
    def pitchfx_metrics_vs_lhb(self):
        return Pitch_Type_Left_View.get_pitchfx_metrics_vs_lhb_for_player(self.db_engine, self.player.id)

    @cached_property
    def pitch_metrics_by_year(self):
        return {
            season.year: Pitch_Type_By_Year_View.get_pitch_metrics_for_year_for_player(
                self.db_engine, self.player.id, season.id
            )
            for season in self.seasons_played
        }

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
        pitch_metrics = self.get_pitch_metrics_for_game(bbref_game_id=game_id)
        return self.get_pitch_mix_batter_stance_splits(
            pitch_metrics["all"], pitch_metrics["vs_rhb"], pitch_metrics["vs_lhb"], include_pitch_count=True
        )

    def get_pitch_mix_batter_stance_splits_for_career(self):
        return self.get_pitch_mix_batter_stance_splits(
            self.pitchfx_metrics_career, self.pitchfx_metrics_vs_rhb, self.pitchfx_metrics_vs_lhb
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
            for pitch_type in pitch_metrics_all.pitch_types
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
            for pitch_type in self.pitchfx_metrics_career.pitch_types
        ]

    def get_pitch_mix_season_splits_for_pitch_type(self, game_id, pitch_type):
        pitch_metrics_for_game = self.get_pitch_metrics_for_game(bbref_game_id=game_id)
        table_row = {
            "pitch_type": pitch_type.print_name,
            "game": pitch_metrics_for_game["all"].get_usage_stats_for_pitch_type(pitch_type),
            "all": self.pitchfx_metrics_career.get_usage_stats_for_pitch_type(pitch_type),
        }
        for year, pitch_metrics_for_year in self.pitch_metrics_by_year.items():
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
        pitch_metrics = self.get_pitch_metrics_for_game(bbref_game_id=game_id)
        return pitch_metrics["all"].get_plate_discipline_pitch_type_splits(include_pitch_count=True)

    def get_plate_discipline_pitch_type_splits_for_career(self):
        return self.pitchfx_metrics_career.get_plate_discipline_pitch_type_splits()

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
        pitch_metrics = self.get_pitch_metrics_for_game(bbref_game_id=game_id)
        return pitch_metrics["all"].get_batted_ball_pitch_type_splits(include_bip_count=True)

    def get_batted_ball_pitch_type_splits_for_career(self):
        return self.pitchfx_metrics_career.get_batted_ball_pitch_type_splits()

    def get_pitch_metrics_for_game(self, bbref_game_id=None, pitch_app_id=None):
        pitch_app = self.get_pitch_app(bbref_game_id, pitch_app_id)
        if not pitch_app:
            return None
        pitch_mix_data = self.pitch_app_dict.get(bbref_game_id)
        if not pitch_mix_data:
            pitch_mix_data = {
                "all": PitchApp_PitchType_All_View.get_pitchfx_metrics_for_pitch_app(self.db_engine, pitch_app.id),
                "vs_rhb": PitchApp_PitchType_Right_View.get_pitchfx_metrics_vs_rhb_for_pitch_app(
                    self.db_engine, pitch_app.id
                ),
                "vs_lhb": PitchApp_PitchType_Left_View.get_pitchfx_metrics_vs_lhb_for_pitch_app(
                    self.db_engine, pitch_app.id
                ),
            }
            self.pitch_app_dict[bbref_game_id] = pitch_mix_data
        return pitch_mix_data

    def get_pitch_app(self, bbref_game_id, pitch_app_id):
        if not pitch_app_id and not bbref_game_id:
            return None
        if pitch_app_id and not bbref_game_id:
            bbref_game_id = validate_pitch_app_id(pitch_app_id).value["game_id"]
        return self.pitch_app_map.get(bbref_game_id)
