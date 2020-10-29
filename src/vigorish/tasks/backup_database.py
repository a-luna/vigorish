import os
from datetime import datetime, timezone
from pathlib import Path

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
                "backup_database_complete",
            )
        )

    def execute(self):
        table_filename_map = self.get_table_filename_map()
        self.events.backup_database_start(len(table_filename_map))
        for table_num, (table, csv_file) in enumerate(table_filename_map.items(), start=1):
            count = last_reported = 0
            total = get_total_number_of_rows(self.db_session, table)
            self.events.backup_table_start(table_num, table)
            with csv_file.open("a") as csv:
                for row in table.export_table_as_csv(self.db_session):
                    csv.write(f"{row}\n")
                    count += 1
                    last_reported = self.report_progress(count, total, last_reported)
            self.events.backup_table_complete(table_num, table)
        self.events.backup_database_complete()
        return Result.Ok(table_filename_map)

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
        if os.environ.get("ENV") == "TEST":
            return Path(backup_folder_path).joinpath("_timestamp_")
        folder_name = f"{datetime.now(timezone.utc).strftime(FILE_TIMESTAMP)}"
        csv_folder = Path(backup_folder_path).joinpath(folder_name)
        csv_folder.mkdir(parents=True)
        return csv_folder

    def report_progress(self, count, total, last_reported):
        res = 0.01
        percent_complete = count / float(total)
        if percent_complete - last_reported > res:
            self.events.backup_table_progress(percent_complete)
            last_reported = percent_complete
        return last_reported
