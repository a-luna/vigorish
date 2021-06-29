from dataclasses import asdict
from functools import cached_property
from pathlib import Path
from zipfile import ZipFile

from dataclass_csv import DataclassReader
from events import Events

import vigorish.database as db
from vigorish.enums import DataSet
from vigorish.tasks.base import Task
from vigorish.util.result import Result
from vigorish.util.string_helpers import (
    get_bbref_team_id,
    get_date_status_id_from_game_date,
    get_game_date_from_bbref_game_id,
)

DATACLASS_CSV_MAP = {
    db.DateScrapeStatusCsvRow: "scrape_status_date.csv",
    db.GameScrapeStatusCsvRow: "scrape_status_game.csv",
    db.PitchAppScrapeStatusCsvRow: "scrape_status_pitch_app.csv",
    db.BatStatsCsvRow: "bat_stats.csv",
    db.PitchStatsCsvRow: "pitch_stats.csv",
    db.PitchFxCsvRow: "pitchfx.csv",
}

RESTORE_TABLE_ORDER = {
    1: (db.DateScrapeStatusCsvRow, db.DateScrapeStatus),
    2: (db.GameScrapeStatusCsvRow, db.GameScrapeStatus),
    3: (db.PitchAppScrapeStatusCsvRow, db.PitchAppScrapeStatus),
    4: (db.BatStatsCsvRow, db.BatStats),
    5: (db.PitchStatsCsvRow, db.PitchStats),
    6: (db.PitchFxCsvRow, db.PitchFx),
}

BATCH_SIZE = 20000


class RestoreDatabaseTask(Task):
    def __init__(self, app):
        super().__init__(app)
        backup_folder_setting = self.config.all_settings.get("DB_BACKUP_FOLDER_PATH")
        self._team_id_map = {}
        self.backup_folder = backup_folder_setting.current_setting(DataSet.ALL).resolve()
        self.csv_folder = None
        self.csv_map = {}
        self.events = Events(
            (
                "unzip_backup_files_start",
                "unzip_backup_files_complete",
                "restore_database_start",
                "restore_table_start",
                "batch_insert_performed",
                "restore_table_complete",
                "restore_database_complete",
            )
        )

    @cached_property
    def season_id_map(self):
        return db.Season.get_regular_season_map(self.db_session)

    @cached_property
    def player_id_map(self):
        return db.PlayerId.get_player_id_map(self.db_session)

    @cached_property
    def game_id_map(self):
        return db.GameScrapeStatus.get_game_id_map(self.db_session)

    @cached_property
    def pitch_app_id_map(self):
        return db.PitchAppScrapeStatus.get_pitch_app_id_map(self.db_session)

    @property
    def update_relationships_map(self):
        return {
            db.DateScrapeStatusCsvRow: self.update_date_status_relationships,
            db.GameScrapeStatusCsvRow: self.update_game_status_relationships,
            db.PitchAppScrapeStatusCsvRow: self.update_pitch_app_status_relationships,
            db.BatStatsCsvRow: self.update_bat_stats_relationships,
            db.PitchStatsCsvRow: self.update_pitch_stats_relationships,
            db.PitchFxCsvRow: self.update_pitchfx_relationships,
        }

    def get_team_id_map_for_year(self, year):
        team_id_map_for_year = self._team_id_map.get(year)
        if not team_id_map_for_year:
            team_id_map_for_year = db.Team.get_team_id_map_for_year(self.db_session, year)
            self._team_id_map[year] = team_id_map_for_year
        return team_id_map_for_year

    def update_date_status_relationships(self, dataclass):
        date_status_dict = asdict(dataclass)
        game_date = date_status_dict["game_date"]
        date_status_dict["season_id"] = self.season_id_map[game_date.year]
        return date_status_dict

    def update_game_status_relationships(self, dataclass):
        game_status_dict = asdict(dataclass)
        game_date = game_status_dict["game_date"]
        game_status_dict["season_id"] = self.season_id_map[game_date.year]
        game_status_dict["scrape_status_date_id"] = get_date_status_id_from_game_date(game_date)
        return game_status_dict

    def update_pitch_app_status_relationships(self, dataclass):
        pitch_app_dict = asdict(dataclass)
        game_date = get_game_date_from_bbref_game_id(pitch_app_dict["bbref_game_id"])
        pitch_app_dict["pitcher_id"] = self.player_id_map[pitch_app_dict["pitcher_id_mlb"]]
        pitch_app_dict["season_id"] = self.season_id_map[game_date.year]
        pitch_app_dict["scrape_status_date_id"] = get_date_status_id_from_game_date(game_date)
        pitch_app_dict["scrape_status_game_id"] = self.game_id_map[pitch_app_dict["bbref_game_id"]]
        return pitch_app_dict

    def update_player_stats_relationships(self, dataclass):
        stats_dict = asdict(dataclass)
        game_date = get_game_date_from_bbref_game_id(stats_dict["bbref_game_id"])
        stats_dict["player_id"] = self.player_id_map[stats_dict["player_id_mlb"]]
        stats_dict["player_team_id"] = self.get_team_id_map_for_year(game_date.year)[stats_dict["player_team_id_bbref"]]
        stats_dict["opponent_team_id"] = self.get_team_id_map_for_year(game_date.year)[
            stats_dict["opponent_team_id_bbref"]
        ]
        stats_dict["season_id"] = self.season_id_map[game_date.year]
        stats_dict["date_id"] = get_date_status_id_from_game_date(game_date)
        stats_dict["game_status_id"] = self.game_id_map[stats_dict["bbref_game_id"]]
        return stats_dict

    def update_bat_stats_relationships(self, dataclass):
        return self.update_player_stats_relationships(dataclass)

    def update_pitch_stats_relationships(self, dataclass):
        pitch_stats_dict = self.update_player_stats_relationships(dataclass)
        pitch_stats_dict["pitch_app_db_id"] = self.pitch_app_id_map[pitch_stats_dict["pitch_app_id"]]
        return pitch_stats_dict

    def update_pitchfx_relationships(self, dataclass):
        pfx_dict = asdict(dataclass)
        game_date = get_game_date_from_bbref_game_id(pfx_dict["bbref_game_id"])
        pitcher_team_id_br = get_bbref_team_id(pfx_dict["pitcher_team_id_bb"])
        opponent_team_id_br = get_bbref_team_id(pfx_dict["opponent_team_id_bb"])
        pfx_dict["pitcher_id"] = self.player_id_map[pfx_dict["pitcher_id_mlb"]]
        pfx_dict["batter_id"] = self.player_id_map[pfx_dict["batter_id_mlb"]]
        pfx_dict["team_pitching_id"] = self.get_team_id_map_for_year(game_date.year)[pitcher_team_id_br]
        pfx_dict["team_batting_id"] = self.get_team_id_map_for_year(game_date.year)[opponent_team_id_br]
        pfx_dict["season_id"] = self.season_id_map[game_date.year]
        pfx_dict["date_id"] = get_date_status_id_from_game_date(game_date)
        pfx_dict["game_status_id"] = self.game_id_map[pfx_dict["bbref_game_id"]]
        pfx_dict["pitch_app_db_id"] = self.pitch_app_id_map[pfx_dict["pitch_app_id"]]
        return pfx_dict

    def execute(self, zip_file=None, csv_folder=None):
        if not zip_file:
            result = self.get_most_recent_backup()
            if result.failure:
                return result
            zip_file = result.value
        self.events.unzip_backup_files_start()
        self.csv_map = self.unzip_csv_files(zip_file)
        self.events.unzip_backup_files_complete()
        result = self.app.prepare_database_for_restore(csv_folder)
        if result.failure:
            return result
        self.events.restore_database_start()
        self.restore_database()
        self.remove_csv_files()
        self.events.restore_database_complete()
        return Result.Ok()

    def get_most_recent_backup(self):
        backup_zip_files = list(Path(self.backup_folder).glob("*.zip"))
        if not backup_zip_files:
            return Result.Fail(f"Error! No backups found in folder:\n{self.backup_folder}")
        backup_zip_files.sort(key=lambda x: x.name, reverse=True)
        return Result.Ok(backup_zip_files[0])

    def unzip_csv_files(self, zip_file):
        with ZipFile(zip_file, mode="r") as zip:
            zip.extractall(path=self.backup_folder)
        self.csv_folder = Path(self.backup_folder).joinpath(zip_file.stem)
        return {dataclass: self.csv_folder.joinpath(csv_file) for dataclass, csv_file in DATACLASS_CSV_MAP.items()}

    def restore_database(self):
        for num in sorted(RESTORE_TABLE_ORDER.keys()):
            (dataclass, db_table) = RESTORE_TABLE_ORDER[num]
            csv_file = self.csv_map[dataclass]
            self.events.restore_table_start(db_table)
            self.restore_table_from_csv(csv_file, dataclass, db_table)
            self.events.restore_table_complete(db_table)

    def restore_table_from_csv(self, csv_file, csv_dataclass, db_table):
        with open(csv_file) as csv:
            reader = DataclassReader(csv, csv_dataclass)
            batch = []
            batch_count = 1
            for csv_row in reader:
                obj_dict = self.update_relationships_map[csv_dataclass](csv_row)
                batch.append(obj_dict)
                if len(batch) < BATCH_SIZE:
                    continue
                self.perform_batch_insert(db_table, batch)
                self.events.batch_insert_performed(batch_count)
                batch_count += 1
            if batch:
                self.perform_batch_insert(db_table, batch)
                self.events.batch_insert_performed(batch_count)

    def perform_batch_insert(self, db_table, batch):
        self.db_engine.execute(db_table.__table__.insert(), batch)
        self.db_session.commit()
        batch.clear()

    def remove_csv_files(self):
        for csv_file in self.csv_map.values():
            csv_file.unlink()
        self.csv_folder.rmdir()
