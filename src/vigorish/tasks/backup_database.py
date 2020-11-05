import os
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from events import Events

from vigorish.config.database import (
    DateScrapeStatus,
    GameScrapeStatus,
    get_total_number_of_rows,
    PitchAppScrapeStatus,
    PitchFx,
)
from vigorish.enums import DataSet
from vigorish.tasks.base import Task
from vigorish.util.dt_format_strings import FILE_TIMESTAMP
from vigorish.util.numeric_helpers import ONE_PERCENT
from vigorish.util.result import Result


class BackupDatabaseTask(Task):
    def __init__(self, app):
        super().__init__(app)
        self.events = Events(
            (
                "backup_database_start",
                "backup_table_start",
                "backup_table_progress",
                "backup_table_complete",
                "create_zip_file_start",
                "create_zip_file_complete",
                "backup_database_complete",
            )
        )

    def execute(self):
        csv_map = self.get_table_filename_map()
        self.events.backup_database_start(len(csv_map))
        for table_num, (table, csv_file) in enumerate(csv_map.items(), start=1):
            self.events.backup_table_start(table_num, table)
            self.export_table_to_csv(table, csv_file)
            self.events.backup_table_complete(table_num, table)
        self.events.create_zip_file_start()
        zip_file = self.create_zip_file(csv_map)
        self.remove_csv_files(csv_map)
        self.events.create_zip_file_complete()
        self.events.backup_database_complete()
        return Result.Ok(zip_file)

    def get_table_filename_map(self):
        csv_folder = self.create_csv_folder()
        return {
            DateScrapeStatus: csv_folder.joinpath("scrape_status_date.csv"),
            GameScrapeStatus: csv_folder.joinpath("scrape_status_game.csv"),
            PitchAppScrapeStatus: csv_folder.joinpath("scrape_status_pitch_app.csv"),
            PitchFx: csv_folder.joinpath("pitchfx.csv"),
        }

    def create_csv_folder(self):
        backup_folder = self.config.all_settings.get("DB_BACKUP_FOLDER_PATH")
        backup_folder_path = backup_folder.current_setting(DataSet.ALL).resolve()
        folder_name = f"{datetime.now(timezone.utc).strftime(FILE_TIMESTAMP)}"
        if os.environ.get("ENV") == "TEST":
            folder_name = "__timestamp__"
        csv_folder = Path(backup_folder_path).joinpath(folder_name)
        csv_folder.mkdir(parents=True)
        return csv_folder

    def export_table_to_csv(self, table, csv_file):
        count = last_reported = 0
        total = get_total_number_of_rows(self.db_session, table)
        with csv_file.open("a") as csv:
            for row in table.export_table_as_csv(self.db_session):
                csv.write(f"{row}\n")
                count += 1
                last_reported = self.report_progress(count, total, last_reported)

    def report_progress(self, count, total, last_reported):
        percent_complete = count / float(total)
        if percent_complete - last_reported > ONE_PERCENT:
            self.events.backup_table_progress(percent_complete)
            last_reported = percent_complete
        return last_reported

    def create_zip_file(self, csv_map):
        backup_folder = csv_map.get(DateScrapeStatus).parent
        zip_filename = f"{backup_folder.name}.zip"
        zip_file = backup_folder.parent.joinpath(zip_filename)
        with ZipFile(zip_file, "w", ZIP_DEFLATED) as zip:
            for csv_file in csv_map.values():
                arcname = f"{backup_folder.name}/{csv_file.name}"
                zip.write(csv_file, arcname=arcname)
        return zip_file

    def remove_csv_files(self, csv_map):
        backup_folder = csv_map.get(DateScrapeStatus).parent
        for csv_file in csv_map.values():
            csv_file.unlink()
        backup_folder.rmdir()
