from dataclasses import asdict
from pathlib import Path
from zipfile import ZipFile

from dataclass_csv import DataclassReader
from events import Events

from vigorish.config.database import (
    DateScrapeStatus,
    GameScrapeStatus,
    PitchAppScrapeStatus,
    PitchFx,
)
from vigorish.enums import DataSet
from vigorish.models.pitchfx import PitchFxCsvRow
from vigorish.models.status_date import DateScrapeStatusCsvRow
from vigorish.models.status_game import GameScrapeStatusCsvRow
from vigorish.models.status_pitch_appearance import PitchAppScrapeStatusCsvRow
from vigorish.tasks.base import Task
from vigorish.util.numeric_helpers import ONE_PERCENT
from vigorish.util.result import Result

CSV_DATACLASS_MAP = {
    "scrape_status_date.csv": DateScrapeStatusCsvRow,
    "scrape_status_game.csv": GameScrapeStatusCsvRow,
    "scrape_status_pitch_app.csv": PitchAppScrapeStatusCsvRow,
    "pitchfx.csv": PitchFxCsvRow,
}

DATACLASS_TABLE_MAP = {
    DateScrapeStatusCsvRow: DateScrapeStatus,
    GameScrapeStatusCsvRow: GameScrapeStatus,
    PitchAppScrapeStatusCsvRow: PitchAppScrapeStatus,
    PitchFxCsvRow: PitchFx,
}


class RestoreDatabaseTask(Task):
    def __init__(self, app):
        super().__init__(app)
        backup_folder_setting = self.config.all_settings.get("DB_BACKUP_FOLDER_PATH")
        self.backup_folder = backup_folder_setting.current_setting(DataSet.ALL).resolve()
        self.csv_map = {}
        self.events = Events(
            (
                "unzip_backup_file_start",
                "unzip_backup_file_complete",
                "restore_database_start",
                "restore_table_start",
                "restore_table_progress",
                "restore_table_complete",
                "restore_database_complete",
            )
        )

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
        for csv_file, dataclass in self.csv_map.items():
            db_table = DATACLASS_TABLE_MAP[dataclass]
            self.events.restore_table_start(db_table)
            result = self.restore_table_from_csv(csv_file, dataclass, db_table)
            self.events.restore_table_complete(db_table)
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
            csv_folder.joinpath(csv_file): dataclass
            for csv_file, dataclass in CSV_DATACLASS_MAP.items()
        }

    def restore_table_from_csv(self, csv_file, csv_dataclass, db_table):
        count = last_reported = 0
        csv_text = csv_file.read_text()
        total = len([row for row in csv_text.split("\n") if row]) - 1
        with open(csv_file) as csv:
            reader = DataclassReader(csv, csv_dataclass)
            for dataclass in reader:
                obj = db_table(**asdict(dataclass))
                obj.update_relationships(self.db_session)
                self.db_session.add(obj)
                count += 1
                last_reported = self.report_progress(count, total, last_reported)

    def report_progress(self, count, total, last_reported):
        percent_complete = count / float(total)
        if percent_complete - last_reported > ONE_PERCENT:
            self.events.restore_table_progress(percent_complete)
            last_reported = percent_complete
        return last_reported

    def remove_csv_files(self):
        backup_folder = list(self.csv_map.keys())[0].parent
        for csv_file in self.csv_map.keys():
            csv_file.unlink()
        backup_folder.rmdir()
