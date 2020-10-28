from datetime import datetime, timezone

from events import Events
from sqlalchemy import func

from vigorish.config.database import (
    DateScrapeStatus,
    GameScrapeStatus,
    PitchAppScrapeStatus,
    PitchFx,
)
from vigorish.config.project_paths import ROOT_FOLDER
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
            total = self.get_total_number_of_rows(table)
            self.events.backup_table_start(table_num, table)
            with csv_file.open("a") as csv:
                for row in table.export_table_as_csv(self.db_session):
                    csv.write(f"{row}\n")
                    count += 1
                    last_reported = self.report_progress(count, total, last_reported)
            self.events.backup_table_complete(table_num, table)
        self.events.backup_database_complete()
        return Result.Ok()

    def get_table_filename_map(self):
        csv_folder = self.create_csv_folder()
        return {
            DateScrapeStatus: csv_folder.joinpath("scrape_status_date.csv"),
            GameScrapeStatus: csv_folder.joinpath("scrape_status_game.csv"),
            PitchAppScrapeStatus: csv_folder.joinpath("scrape_status_pitch_app.csv"),
            PitchFx: csv_folder.joinpath("pitchfx.csv"),
        }

    def create_csv_folder(self):
        backup_folder = self.config.all_settings.get("DB_BACKUP_FOLDER_PATH", "backup")
        folder_name = f"{datetime.now(timezone.utc).strftime(FILE_TIMESTAMP)}"
        csv_folder = ROOT_FOLDER.joinpath(backup_folder).joinpath(folder_name)
        csv_folder.mkdir(parents=True)
        return csv_folder

    def get_total_number_of_rows(self, db_table):
        q = self.db_session.query(db_table)
        count_q = q.statement.with_only_columns([func.count()]).order_by(None)
        count = q.session.execute(count_q).scalar()
        return count

    def report_progress(self, count, total, last_reported):
        res = 0.01
        percent_complete = count / float(total)
        if percent_complete - last_reported > res:
            self.events.backup_table_progress(percent_complete)
            last_reported = percent_complete
        return last_reported
