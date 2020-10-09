"""Menu item that returns the user to the previous menu."""
import subprocess

from getch import pause
from halo import Halo

from vigorish.cli.components import (
    get_random_cli_color,
    get_random_dots_spinner,
    print_heading,
    print_message,
)
from vigorish.enums import DataSet, SyncDirection, VigFile
from vigorish.tasks.base import Task
from vigorish.tasks.sync_scraped_data import SyncScrapedData
from vigorish.util.result import Result

SYNC_STATUS_TEXT_COLOR = {
    "out_of_sync": "bright_green",
    "in_sync": "blue",
    "sync_complete": "bright_green",
    "error": "bright_red",
}


class SyncScrapedDataNoPrompts(Task):
    def __init__(self, app):
        super().__init__(app)
        self.sync_direction = None
        self.year = None
        self.file_type = None
        self.data_sets_int = 0
        self.current_data_set = None
        self.task_number = 0
        self.sync_files = {}
        self.sync_results = []
        self.task_number = 0
        self.spinners = {}
        self.results = {}

    @property
    def bbref_games_for_date(self):
        return self.data_sets_int & DataSet.BBREF_GAMES_FOR_DATE == DataSet.BBREF_GAMES_FOR_DATE

    @property
    def bbref_boxscores(self):
        return self.data_sets_int & DataSet.BBREF_BOXSCORES == DataSet.BBREF_BOXSCORES

    @property
    def brooks_games_for_date(self):
        return self.data_sets_int & DataSet.BROOKS_GAMES_FOR_DATE == DataSet.BROOKS_GAMES_FOR_DATE

    @property
    def brooks_pitch_logs(self):
        return self.data_sets_int & DataSet.BROOKS_PITCH_LOGS == DataSet.BROOKS_PITCH_LOGS

    @property
    def brooks_pitchfx(self):
        return self.data_sets_int & DataSet.BROOKS_PITCHFX == DataSet.BROOKS_PITCHFX

    @property
    def data_sets(self):
        if self.file_type == VigFile.COMBINED_GAME_DATA:
            return [DataSet.ALL]
        data_sets = []
        if self.brooks_games_for_date:
            data_sets.append(DataSet.BROOKS_GAMES_FOR_DATE)
        if self.brooks_pitch_logs:
            data_sets.append(DataSet.BROOKS_PITCH_LOGS)
        if self.brooks_pitchfx:
            data_sets.append(DataSet.BROOKS_PITCHFX)
        if self.bbref_games_for_date:
            data_sets.append(DataSet.BBREF_GAMES_FOR_DATE)
        if self.bbref_boxscores:
            data_sets.append(DataSet.BBREF_BOXSCORES)
        return sorted(ds for ds in data_sets if ds in self.valid_data_sets())

    @property
    def total_tasks(self):
        return len(self.data_sets)

    @property
    def all_files_are_in_sync(self):
        return all(not out_of_sync for (out_of_sync, _, _) in self.sync_files.values())

    def execute(self, sync_direction, year, file_type, data_sets_int):
        self.sync_direction = sync_direction
        self.year = year
        self.file_type = file_type
        self.data_sets_int = data_sets_int
        self.initialize_s3_sync_task()
        for data_set in self.data_sets:
            self.task_number += 1
            self.current_data_set = data_set
            self.report_sync_results()
            result = self.s3_sync.execute(sync_direction, file_type, data_set, year)
            self.results[data_set] = result
            self.spinners[data_set].stop()
            if result.failure:
                return self.results
        self.report_sync_results()
        self.teardown()
        pause(message="\nPress any key to continue...")
        return self.results

    def initialize_s3_sync_task(self):
        s3_objects = self.get_all_objects_in_s3()
        self.s3_sync = SyncScrapedData(self.app, cached_s3_objects=s3_objects)
        self.s3_sync.events.error_occurred += self.error_occurred
        self.s3_sync.events.get_sync_files_start += self.get_sync_files_start
        self.s3_sync.events.get_sync_files_complete += self.get_sync_files_complete
        self.s3_sync.events.sync_files_start += self.sync_files_start
        self.s3_sync.events.sync_files_progress += self.sync_files_progress
        self.s3_sync.events.sync_files_complete += self.sync_files_complete

    def get_all_objects_in_s3(self):
        subprocess.run(["clear"])
        spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        spinner.text = "Retrieving data for all objects stored in S3..."
        spinner.start()
        s3_obj_collection = self.scraped_data.file_helper.get_all_object_keys_in_s3_bucket()
        s3_objects = [obj for obj in s3_obj_collection]
        spinner.stop()
        return s3_objects

    def valid_data_sets(self):
        data_set_file_type_map = {
            VigFile.SCRAPED_HTML: [
                DataSet.BBREF_GAMES_FOR_DATE,
                DataSet.BROOKS_GAMES_FOR_DATE,
                DataSet.BBREF_BOXSCORES,
                DataSet.BROOKS_PITCH_LOGS,
                DataSet.BROOKS_PITCHFX,
            ],
            VigFile.PARSED_JSON: [
                DataSet.BBREF_GAMES_FOR_DATE,
                DataSet.BROOKS_GAMES_FOR_DATE,
                DataSet.BBREF_BOXSCORES,
                DataSet.BROOKS_PITCH_LOGS,
                DataSet.BROOKS_PITCHFX,
            ],
            VigFile.COMBINED_GAME_DATA: [DataSet.ALL],
            VigFile.PATCH_LIST: [DataSet.BBREF_BOXSCORES, DataSet.BROOKS_PITCHFX],
        }
        return data_set_file_type_map[self.file_type]

    def report_sync_results(self):
        subprocess.run(["clear"])
        self.print_header_message()
        if not self.sync_results:
            return
        for task_result in self.sync_results:
            print_message(task_result[0], wrap=False, fg=task_result[1])

    def print_header_message(self):
        if self.sync_direction == SyncDirection.UP_TO_S3:
            heading = "Syncing data from local folder to S3 bucket"
        if self.sync_direction == SyncDirection.DOWN_TO_LOCAL:
            heading = "Syncing data from S3 bucket to local folder"
        print_heading(heading, fg="bright_yellow")

    def teardown(self):
        self.s3_sync.events.error_occurred -= self.error_occurred
        self.s3_sync.events.get_sync_files_start -= self.get_sync_files_start
        self.s3_sync.events.get_sync_files_complete -= self.get_sync_files_complete
        self.s3_sync.events.sync_files_start -= self.sync_files_start
        self.s3_sync.events.sync_files_progress -= self.sync_files_progress
        self.s3_sync.events.sync_files_complete -= self.sync_files_complete

    def error_occurred(self, error_message):
        self.sync_results.append((error_message, SYNC_STATUS_TEXT_COLOR["error"]))
        self.results[self.current_data_set] = Result.Fail(error_message)

    def get_sync_files_start(self):
        self.spinners[self.current_data_set] = Halo(
            spinner=get_random_dots_spinner(),
            color=get_random_cli_color(),
            text=self.sync_started_text(),
        )
        self.spinners[self.current_data_set].start()

    def sync_started_text(self):
        return (
            f"Analyzing MLB {self.year} {self.file_type} {self.current_data_set} files "
            f"(Task {self.task_number}/{self.total_tasks})..."
        )

    def get_sync_files_complete(self, sync_results):
        (out_of_sync, new_files, update_files) = sync_results
        self.sync_files[self.current_data_set] = (out_of_sync, new_files, update_files)
        if out_of_sync:
            sync_count = len(new_files) + len(update_files)
            self.sync_results.append(self.out_of_sync_text(sync_count))
        else:
            self.sync_results.append(self.in_sync_text())

    def out_of_sync_text(self, sync_count):
        out_of_sync = (
            f"[{self.year} {self.file_type} {self.current_data_set}] "
            f"{sync_count} files out of sync! "
            f"(Task {self.task_number}/{self.total_tasks})"
        )
        return (out_of_sync, SYNC_STATUS_TEXT_COLOR["out_of_sync"])

    def in_sync_text(self):
        in_sync = (
            f"[{self.year} {self.file_type} {self.current_data_set}] "
            f"All files in sync "
            f"(Task {self.task_number}/{self.total_tasks})"
        )
        return (in_sync, SYNC_STATUS_TEXT_COLOR["in_sync"])

    def sync_files_start(self, name, complete, total):
        self.report_sync_results()
        self.sync_files_progress(name, complete, total)

    def sync_files_progress(self, name, complete, total):
        direction = "Down" if self.sync_direction == "SYNC_DOWN" else "Up"
        percent = complete / float(total)
        progress_message = f"{direction}loading: {name} | {percent:.0%} ({complete}/{total} Files)"
        self.spinners[self.current_data_set].text = progress_message

    def sync_files_complete(self):
        if self.sync_direction == SyncDirection.UP_TO_S3:
            sync_complete = (
                f"[{self.year} {self.file_type} {self.current_data_set}] "
                f"All changes have been synced to s3 bucket!"
            )
        if self.sync_direction == SyncDirection.DOWN_TO_LOCAL:
            sync_complete = (
                f"[{self.year} {self.file_type} {self.current_data_set}] "
                f"All changes have been synced to local folder!"
            )
        self.sync_results.append((sync_complete, SYNC_STATUS_TEXT_COLOR["sync_complete"]))
