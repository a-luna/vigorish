import os
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from events import Events

import vigorish.database as db
from vigorish.tasks.base import Task
from vigorish.util.dataclass_helpers import serialize_db_object_to_csv
from vigorish.util.dt_format_strings import CSV_UTC, DATE_ONLY, DT_AWARE, FILE_TIMESTAMP
from vigorish.util.numeric_helpers import ONE_PERCENT
from vigorish.util.result import Result

DB_MODEL_TO_CSV_MAP = {
    db.DateScrapeStatus: {"dataclass": db.DateScrapeStatusCsvRow, "date_format": DATE_ONLY},
    db.GameScrapeStatus: {"dataclass": db.GameScrapeStatusCsvRow, "date_format": DATE_ONLY},
    db.PitchAppScrapeStatus: {"dataclass": db.PitchAppScrapeStatusCsvRow, "date_format": DT_AWARE},
    db.BatStats: {"dataclass": db.BatStatsCsvRow, "date_format": None},
    db.PitchStats: {"dataclass": db.PitchStatsCsvRow, "date_format": None},
    db.PitchFx: {"dataclass": db.PitchFxCsvRow, "date_format": CSV_UTC},
}


class BackupDatabaseTask(Task):
    def __init__(self, app):
        super().__init__(app)
        self.csv_folder = None
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
        csv_map = self.get_csv_map()
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

    def get_csv_map(self):
        self.csv_folder = self.create_csv_folder()
        return {
            db.DateScrapeStatus: self.csv_folder.joinpath("scrape_status_date.csv"),
            db.GameScrapeStatus: self.csv_folder.joinpath("scrape_status_game.csv"),
            db.PitchAppScrapeStatus: self.csv_folder.joinpath("scrape_status_pitch_app.csv"),
            db.BatStats: self.csv_folder.joinpath("bat_stats.csv"),
            db.PitchStats: self.csv_folder.joinpath("pitch_stats.csv"),
            db.PitchFx: self.csv_folder.joinpath("pitchfx.csv"),
        }

    def create_csv_folder(self):
        backup_folder = self.app.get_current_setting("DB_BACKUP_FOLDER_PATH")
        folder_name = f"{datetime.now(timezone.utc).strftime(FILE_TIMESTAMP)}"
        if os.environ.get("ENV") == "TEST":
            folder_name = "__timestamp__"
        csv_folder = Path(backup_folder).joinpath(folder_name)
        csv_folder.mkdir(parents=True)
        return csv_folder

    def export_table_to_csv(self, table, csv_file):
        self.append_to_csv(csv_file, text=self.get_csv_column_names(table))
        total_rows = self.app.get_total_number_of_rows(table)
        chunk_size, chunk_count, row_count, last_reported = 10000, 0, 0, 0
        while True:
            start = chunk_size * chunk_count
            stop = min(chunk_size * (chunk_count + 1), total_rows)
            table_chunk = self.db_session.query(table).slice(start, stop).all()
            if not table_chunk:
                break
            self.append_to_csv(csv_file, text=self.create_csv_rows(table_chunk, table))
            row_count += len(table_chunk)
            chunk_count += 1
            last_reported = self.report_progress(row_count, total_rows, last_reported)
            if row_count == total_rows:
                break

    def append_to_csv(self, csv_file, text):
        with csv_file.open("a") as csv:
            csv.write(f"{text}\n")

    def get_csv_column_names(self, table):
        csv_dataclass = DB_MODEL_TO_CSV_MAP[table]["dataclass"]
        return ",".join(list(csv_dataclass.__dataclass_fields__.keys()))

    def create_csv_rows(self, table_chunk, table):
        csv_rows = [
            serialize_db_object_to_csv(
                db_obj,
                dataclass=DB_MODEL_TO_CSV_MAP[table]["dataclass"],
                date_format=DB_MODEL_TO_CSV_MAP[table]["date_format"],
            )
            for db_obj in table_chunk
        ]
        return "\n".join(csv_rows)

    def report_progress(self, count, total, last_reported):
        percent_complete = count / float(total)
        if percent_complete - last_reported > ONE_PERCENT:
            self.events.backup_table_progress(percent_complete)
            last_reported = percent_complete
        return last_reported

    def create_zip_file(self, csv_map):
        zip_filename = f"{self.csv_folder.name}.zip"
        zip_file = self.csv_folder.parent.joinpath(zip_filename)
        with ZipFile(zip_file, "w", ZIP_DEFLATED) as zip:
            for csv_file in csv_map.values():
                arcname = f"{self.csv_folder.name}/{csv_file.name}"
                zip.write(csv_file, arcname=arcname)
        return zip_file

    def remove_csv_files(self, csv_map):
        for csv_file in csv_map.values():
            csv_file.unlink()
        self.csv_folder.rmdir()
