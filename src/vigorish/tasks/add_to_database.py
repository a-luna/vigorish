from datetime import datetime

from events import Events

from vigorish.data.all_game_data import AllGameData
from vigorish.database import (
    BatStats,
    PitchStats,
    GameScrapeStatus,
    PitchAppScrapeStatus,
    PitchFx,
    PlayerId,
    Season,
    Team,
)
from vigorish.tasks.base import Task
from vigorish.util.dt_format_strings import DATE_ONLY_TABLE_ID
from vigorish.util.result import Result
from vigorish.util.string_helpers import get_bbref_team_id, validate_bbref_game_id


class AddToDatabaseTask(Task):
    def __init__(self, app):
        super().__init__(app)
        self._season_id_map = {}
        self._team_id_map = {}
        self._player_id_map = {}
        self._game_id_map = {}
        self._pitch_app_id_map = {}
        self.events = Events(
            (
                "add_data_to_db_start",
                "add_data_to_db_progress",
                "add_data_to_db_complete",
            )
        )

    @property
    def season_id_map(self):
        if self._season_id_map:
            return self._season_id_map
        self._season_id_map = Season.regular_season_map(self.db_session)
        return self._season_id_map

    def get_team_id_map(self, year):
        if year in self._team_id_map:
            return self._team_id_map[year]
        self._team_id_map[year] = Team.get_team_id_map_for_year(self.db_session, year)
        return self._team_id_map[year]

    @property
    def player_id_map(self):
        if self._player_id_map:
            return self._player_id_map
        self._player_id_map = PlayerId.get_mlb_player_id_map(self.db_session)
        return self._player_id_map

    @property
    def game_id_map(self):
        if self._game_id_map:
            return self._game_id_map
        self._game_id_map = GameScrapeStatus.get_game_id_map(self.db_session)
        return self._game_id_map

    @property
    def pitch_app_id_map(self):
        if self._pitch_app_id_map:
            return self._pitch_app_id_map
        self._pitch_app_id_map = PitchAppScrapeStatus.get_pitch_app_id_map(self.db_session)
        return self._pitch_app_id_map

    def execute(self, audit_report, year=None):
        self.audit_report = audit_report
        return self.add_data_for_year(year) if year else self.add_all_data()

    def add_all_data(self):
        valid_years = [year for year, results in self.audit_report.items() if results["successful"]]
        for year in valid_years:
            self.add_data_for_year(year)
        return Result.Ok()

    def add_data_for_year(self, year):
        report_for_season = self.audit_report.get(year)
        if not report_for_season:
            return Result.Fail(f"Audit report could not be generated for MLB Season {year}")
        game_ids = report_for_season.get("successful")
        if not game_ids:
            error = f"No games for MLB Season {year} qualify to have PitchFx data imported."
            return Result.Fail(error)
        self.events.add_data_to_db_start(year, game_ids)
        for num, game_id in enumerate(game_ids, start=1):
            all_game_data = AllGameData(self.app, game_id)
            result = self.add_player_stats_to_database(game_id, all_game_data)
            if result.failure:
                return result
            result = self.add_pitchfx_to_database(all_game_data)
            if result.failure:
                return result
            self.events.add_data_to_db_progress(num, year, game_id)
        self.events.add_data_to_db_complete(year)
        return Result.Ok()

    def add_player_stats_to_database(self, game_id, all_game_data):
        game_status = GameScrapeStatus.find_by_bbref_game_id(self.db_session, game_id)
        if not game_status:
            error = f"Import aborted! Game status '{game_id}' not found in database"
            return Result.Fail(error)
        if not game_status.imported_bat_stats:
            self.add_bat_stats_to_database(game_id, all_game_data, game_status)
        if not game_status.imported_pitch_stats:
            self.add_pitch_stats_to_database(game_id, all_game_data, game_status)
        return Result.Ok()

    def add_bat_stats_to_database(self, game_id, all_game_data, game_status):
        for mlb_id in all_game_data.bat_stats_player_ids:
            bat_stats_dict = all_game_data.get_bat_stats(mlb_id).value
            bat_stats = BatStats.from_dict(game_id, bat_stats_dict)
            bat_stats = self.update_player_stats_relationships(bat_stats)
            self.db_session.add(bat_stats)
        game_status.imported_bat_stats = 1
        self.db_session.commit()

    def add_pitch_stats_to_database(self, game_id, all_game_data, game_status):
        for mlb_id in all_game_data.pitch_stats_player_ids:
            pitch_stats_dict = all_game_data.get_pitch_app_stats(mlb_id).value
            pitch_stats = PitchStats.from_dict(game_id, pitch_stats_dict)
            pitch_stats = self.update_player_stats_relationships(pitch_stats)
            self.db_session.add(pitch_stats)
        game_status.imported_pitch_stats = 1
        self.db_session.commit()

    def update_player_stats_relationships(self, stats):
        game_date = self.get_game_date_from_bbref_game_id(stats.bbref_game_id)
        stats.player_id = self.player_id_map[stats.player_id_mlb]
        stats.player_team_id = self.get_team_id_map(game_date.year)[stats.player_team_id_bbref]
        stats.opponent_team_id = self.get_team_id_map(game_date.year)[stats.opponent_team_id_bbref]
        stats.season_id = self.season_id_map[game_date.year]
        stats.date_id = self.get_date_status_id_from_game_date(game_date)
        stats.game_status_id = self.game_id_map[stats.bbref_game_id]
        return stats

    def add_pitchfx_to_database(self, all_game_data):
        for pitch_app_id, pfx_dict_list in all_game_data.get_all_pitchfx().items():
            pitch_app = PitchAppScrapeStatus.find_by_pitch_app_id(self.db_session, pitch_app_id)
            if not pitch_app:
                error = f"Import aborted! Pitch app '{pitch_app_id}' not found in database"
                return Result.Fail(error)
            if pitch_app.imported_pitchfx:
                continue
            for pfx_dict in pfx_dict_list:
                pfx = PitchFx.from_dict(pfx_dict)
                pfx = self.update_pitchfx_relationships(pfx)
                self.db_session.add(pfx)
            pitch_app.imported_pitchfx = 1
        self.db_session.commit()
        return Result.Ok()

    def update_pitchfx_relationships(self, pfx):
        game_date = self.get_game_date_from_bbref_game_id(pfx.bbref_game_id)
        pitcher_team_id_br = get_bbref_team_id(pfx.pitcher_team_id_bb)
        opponent_team_id_br = get_bbref_team_id(pfx.opponent_team_id_bb)
        pfx.pitcher_id = self.player_id_map[pfx.pitcher_id_mlb]
        pfx.batter_id = self.player_id_map[pfx.batter_id_mlb]
        pfx.team_pitching_id = self.get_team_id_map(game_date.year)[pitcher_team_id_br]
        pfx.team_batting_id = self.get_team_id_map(game_date.year)[opponent_team_id_br]
        pfx.season_id = self.season_id_map[game_date.year]
        pfx.date_id = self.get_date_status_id_from_game_date(game_date)
        pfx.game_status_id = self.game_id_map[pfx.bbref_game_id]
        pfx.pitch_app_db_id = self.pitch_app_id_map[pfx.pitch_app_id]
        return pfx

    def get_game_date_from_bbref_game_id(self, bbref_game_id):
        game_date = validate_bbref_game_id(bbref_game_id).value["game_date"]
        return datetime(game_date.year, game_date.month, game_date.day)

    def get_date_status_id_from_game_date(self, game_date):
        return game_date.strftime(DATE_ONLY_TABLE_ID)