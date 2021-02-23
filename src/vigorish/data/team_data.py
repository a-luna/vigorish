from functools import cached_property

import vigorish.database as db


class TeamData:
    def __init__(self, app, team_id_bbref, year):
        self.app = app
        self.db_engine = self.app.db_engine
        self.db_session = self.app.db_session
        self.team_id_bbref = team_id_bbref
        self.year = year
        self.team = db.Team.find_by_team_id_and_year(self.db_session, self.team_id_bbref, self.year)

    @cached_property
    def team_pitch_stats(self):
        return db.Team_PitchStats_By_Year_View.get_pitch_stats_for_team(self.db_engine, self.team.id)

    @cached_property
    def team_pitch_stats_by_year(self):
        return db.Team_PitchStats_By_Year_View.get_pitch_stats_by_year_for_team(self.db_engine, self.team_id_bbref)

    @cached_property
    def team_pitch_stats_by_player(self):
        return db.Team_PitchStats_By_Player_Year_View.get_pitch_stats_by_player_by_year_for_team(
            self.db_engine, self.team.id
        )

    @cached_property
    def team_bat_stats(self):
        return db.Team_BatStats_By_Year_View.get_bat_stats_for_team(self.db_engine, self.team.id)

    @cached_property
    def team_bat_stats_by_year(self):
        return db.Team_BatStats_By_Year_View.get_bat_stats_by_year_for_team(self.db_engine, self.team_id_bbref)

    @cached_property
    def team_bat_stats_by_player(self):
        return db.Team_BatStats_By_Player_Year_View.get_bat_stats_by_player_by_year_for_team(
            self.db_engine, self.team.id
        )
