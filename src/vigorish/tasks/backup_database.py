import os
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from events import Events

from vigorish.database import (
    DateScrapeStatus,
    DateScrapeStatusCsvRow,
    GameScrapeStatus,
    GameScrapeStatusCsvRow,
    get_total_number_of_rows,
    PitchAppScrapeStatus,
    PitchAppScrapeStatusCsvRow,
    PitchFx,
    PitchFxCsvRow,
)
from vigorish.enums import DataSet
from vigorish.tasks.base import Task
from vigorish.util.dt_format_strings import CSV_UTC, DATE_ONLY, DT_AWARE, FILE_TIMESTAMP
from vigorish.util.dataclass_helpers import dict_from_dataclass, sanitize_row_dict
from vigorish.util.numeric_helpers import ONE_PERCENT
from vigorish.util.result import Result

DB_MODEL_TO_CSV_MAP = {
    DateScrapeStatus: {"dataclass": DateScrapeStatusCsvRow, "date_format": DATE_ONLY},
    GameScrapeStatus: {"dataclass": GameScrapeStatusCsvRow, "date_format": DATE_ONLY},
    PitchAppScrapeStatus: {"dataclass": PitchAppScrapeStatusCsvRow, "date_format": DT_AWARE},
    PitchFx: {"dataclass": PitchFxCsvRow, "date_format": CSV_UTC},
}


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
        chunk_size = 10000
        total_rows = get_total_number_of_rows(self.db_session, table)
        chunk_count, row_count, last_reported = 0, 0, 0
        self.append_text_to_csv_file(csv_file, text=",".join(self.get_csv_column_names(table)))
        while True:
            start = chunk_size * chunk_count
            stop = min(chunk_size * (chunk_count + 1), total_rows)
            table_chunk = self.db_session.query(table).slice(start, stop).all()
            if table_chunk is None:
                break
            csv_dicts = (self.convert_row_to_csv_dict(row, table) for row in table_chunk)
            csv_rows = (",".join(sanitize_row_dict(d, date_format=CSV_UTC)) for d in csv_dicts)
            self.append_text_to_csv_file(csv_file, text="\n".join(csv_rows))
            row_count += len(table_chunk)
            chunk_count += 1
            last_reported = self.report_progress(row_count, total_rows, last_reported)
            if row_count == total_rows:
                break

    def append_text_to_csv_file(self, csv_file, text):
        with csv_file.open("a") as csv:
            csv.write(f"{text}\n")

    def get_csv_column_names(self, table):
        csv_dataclass = DB_MODEL_TO_CSV_MAP[table]["dataclass"]
        return list(csv_dataclass.__dataclass_fields__.keys())

    def convert_row_to_csv_dict(self, row, table):
        csv_dataclass = DB_MODEL_TO_CSV_MAP[table]["dataclass"]
        date_format = DB_MODEL_TO_CSV_MAP[table]["date_format"]
        if isinstance(row, PitchFx):
            row.pdes = row.pdes.replace(",", ";")
        return dict_from_dataclass(row, csv_dataclass, date_format)

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
