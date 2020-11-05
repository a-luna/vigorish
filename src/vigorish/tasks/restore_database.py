from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile

from dataclass_csv import DataclassReader
from events import Events

from vigorish.config.database import (
    DateScrapeStatus,
    GameScrapeStatus,
    PitchAppScrapeStatus,
    PitchFx,
    PlayerId,
    Season,
    Team,
)
from vigorish.enums import DataSet
from vigorish.models.pitchfx import PitchFxCsvRow
from vigorish.models.status_date import DateScrapeStatusCsvRow
from vigorish.models.status_game import GameScrapeStatusCsvRow
from vigorish.models.status_pitch_appearance import PitchAppScrapeStatusCsvRow
from vigorish.tasks.base import Task
from vigorish.util.dt_format_strings import DATE_ONLY_TABLE_ID
from vigorish.util.result import Result
from vigorish.util.string_helpers import get_bbref_team_id, validate_bbref_game_id

DATACLASS_CSV_MAP = {
    DateScrapeStatusCsvRow: "scrape_status_date.csv",
    GameScrapeStatusCsvRow: "scrape_status_game.csv",
    PitchAppScrapeStatusCsvRow: "scrape_status_pitch_app.csv",
    PitchFxCsvRow: "pitchfx.csv",
}

DATACLASS_TABLE_MAP = {
    DateScrapeStatusCsvRow: DateScrapeStatus,
    GameScrapeStatusCsvRow: GameScrapeStatus,
    PitchAppScrapeStatusCsvRow: PitchAppScrapeStatus,
    PitchFxCsvRow: PitchFx,
}

RESTORE_TABLE_ORDER = {
    1: DateScrapeStatusCsvRow,
    2: GameScrapeStatusCsvRow,
    3: PitchAppScrapeStatusCsvRow,
    4: PitchFxCsvRow,
}

BATCH_SIZE = 20000


class RestoreDatabaseTask(Task):
    def __init__(self, app):
        super().__init__(app)
        backup_folder_setting = self.config.all_settings.get("DB_BACKUP_FOLDER_PATH")
        self._season_id_map = {}
        self._team_id_map = {}
        self._player_id_map = {}
        self._game_id_map = {}
        self._pitch_app_id_map = {}
        self.backup_folder = backup_folder_setting.current_setting(DataSet.ALL).resolve()
        self.csv_map = {}
        self.events = Events(
            (
                "unzip_backup_file_start",
                "unzip_backup_file_complete",
                "restore_database_start",
                "restore_table_start",
                "batch_insert_performed",
                "restore_table_complete",
                "restore_database_complete",
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
        self._player_id_map = PlayerId.get_player_id_map(self.db_session)
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

    def execute(self, zip_file=None):
        if not zip_file:
            result = self.get_most_recent_backup()
            if result.failure:
                return result
            zip_file = result.value
        self.events.unzip_backup_file_start()
        self.csv_map = self.unzip_csv_files(zip_file)
        self.events.unzip_backup_file_complete()
        self.events.restore_database_start()
        self.restore_database()
        self.remove_csv_files()
        self.events.restore_database_complete()
        return Result.Ok()

    def get_most_recent_backup(self):
        backup_zip_files = [zip_file for zip_file in Path(self.backup_folder).glob("*.zip")]
        if not backup_zip_files:
            return Result.Fail(f"Error! No backups found in folder:\n{self.backup_folder}")
        backup_zip_files.sort(key=lambda x: x.name, reverse=True)
        return Result.Ok(backup_zip_files[0])

    def unzip_csv_files(self, zip_file):
        with ZipFile(zip_file, mode="r") as zip:
            zip.extractall(path=self.backup_folder)
        csv_folder = Path(self.backup_folder).joinpath(zip_file.stem)
        return {
            dataclass: csv_folder.joinpath(csv_file)
            for dataclass, csv_file in DATACLASS_CSV_MAP.items()
        }

    def restore_database(self):
        for dataclass in RESTORE_TABLE_ORDER.values():
            db_table = DATACLASS_TABLE_MAP[dataclass]
            csv_file = self.csv_map[dataclass]
            self.events.restore_table_start(db_table)
            self.restore_table_from_csv(csv_file, dataclass, db_table)
            self.events.restore_table_complete(db_table)

    def restore_table_from_csv(self, csv_file, csv_dataclass, db_table):
        batch = []
        batch_count = 0
        with open(csv_file) as csv:
            reader = DataclassReader(csv, csv_dataclass)
            for row in reader:
                obj_dict = self.update_db_relationships(csv_dataclass, row)
                batch.append(obj_dict)
                if len(batch) < BATCH_SIZE:
                    continue
                self.db_engine.execute(db_table.__table__.insert(), batch)
                batch.clear()
                batch_count += 1
                self.events.batch_insert_performed(batch_count)
            self.db_engine.execute(db_table.__table__.insert(), batch)
        self.db_session.commit()

    def update_db_relationships(self, csv_dataclass, dataclass):
        update_relationships_map = {
            DateScrapeStatusCsvRow: self.update_date_status_relationships,
            GameScrapeStatusCsvRow: self.update_game_status_relationships,
            PitchAppScrapeStatusCsvRow: self.update_pitch_app_status_relationships,
            PitchFxCsvRow: self.update_pitchfx_relationships,
        }
        return update_relationships_map[csv_dataclass](dataclass)

    def update_date_status_relationships(self, dataclass):
        date_status_dict = asdict(dataclass)
        game_date = date_status_dict["game_date"]
        date_status_dict["season_id"] = self.season_id_map[game_date.year]
        return date_status_dict

    def update_game_status_relationships(self, dataclass):
        game_status_dict = asdict(dataclass)
        game_date = game_status_dict["game_date"]
        game_status_dict["season_id"] = self.season_id_map[game_date.year]
        game_status_dict["scrape_status_date_id"] = self.get_date_status_id_from_game_date(
            game_date
        )
        return game_status_dict

    def update_pitch_app_status_relationships(self, dataclass):
        pitch_app_dict = asdict(dataclass)
        game_date = self.get_game_date_from_bbref_game_id(pitch_app_dict["bbref_game_id"])
        pitch_app_dict["pitcher_id"] = self.player_id_map[pitch_app_dict["pitcher_id_mlb"]]
        pitch_app_dict["season_id"] = self.season_id_map[game_date.year]
        pitch_app_dict["scrape_status_date_id"] = self.get_date_status_id_from_game_date(game_date)
        pitch_app_dict["scrape_status_game_id"] = self.game_id_map[pitch_app_dict["bbref_game_id"]]
        return pitch_app_dict

    def update_pitchfx_relationships(self, dataclass):
        pfx_dict = asdict(dataclass)
        game_date = self.get_game_date_from_bbref_game_id(pfx_dict["bbref_game_id"])
        pitcher_team_id_br = get_bbref_team_id(pfx_dict["pitcher_team_id_bb"])
        opponent_team_id_br = get_bbref_team_id(pfx_dict["opponent_team_id_bb"])
        pfx_dict["pitcher_id"] = self.player_id_map[pfx_dict["pitcher_id_mlb"]]
        pfx_dict["batter_id"] = self.player_id_map[pfx_dict["batter_id_mlb"]]
        pfx_dict["team_pitching_id"] = self.get_team_id_map(game_date.year)[pitcher_team_id_br]
        pfx_dict["team_batting_id"] = self.get_team_id_map(game_date.year)[opponent_team_id_br]
        pfx_dict["season_id"] = self.season_id_map[game_date.year]
        pfx_dict["date_id"] = self.get_date_status_id_from_game_date(game_date)
        pfx_dict["game_status_id"] = self.game_id_map[pfx_dict["bbref_game_id"]]
        pfx_dict["pitch_app_db_id"] = self.pitch_app_id_map[pfx_dict["pitch_app_id"]]
        return pfx_dict

    def get_game_date_from_bbref_game_id(self, bbref_game_id):
        game_date = validate_bbref_game_id(bbref_game_id).value["game_date"]
        return datetime(game_date.year, game_date.month, game_date.day)

    def get_date_status_id_from_game_date(self, game_date):
        return game_date.strftime(DATE_ONLY_TABLE_ID)

    def remove_csv_files(self):
        backup_folder = list(self.csv_map.values())[0].parent
        for csv_file in self.csv_map.values():
            csv_file.unlink()
        backup_folder.rmdir()
