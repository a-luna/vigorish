"""Menu item that returns the user to the previous menu."""
import subprocess

from halo import Halo

from vigorish.cli.components import (
    get_random_cli_color,
    get_random_dots_spinner,
    print_heading,
    print_message,
)
from vigorish.enums import DataSet, SyncDirection, VigFile
from vigorish.tasks.base import Task
from vigorish.tasks.sync_scraped_data import SyncScrapedDataTask
from vigorish.util.result import Result

SYNC_STATUS_TEXT_COLOR = {
    "out_of_sync": "bright_green",
    "in_sync": "blue",
    "sync_complete": "bright_green",
    "error": "bright_red",
}


class SyncDataNoPromptsTask(Task):
    def __init__(self, app):
        super().__init__(app)
        self.s3_sync = SyncScrapedDataTask(self.app)
        self.sync_direction = None
        self.year = None
        self.file_type = None
        self.data_sets_int = 0
        self.data_set = None
        self.task_number = 0
        self.sync_files = {}
        self.sync_results = []
        self.spinners = {}
        self.results = {}

    @property
    def data_sets(self):
        if self.file_type == VigFile.COMBINED_GAME_DATA:
            return [DataSet.ALL]
        return sorted(ds for ds in DataSet if ds in self.valid_data_sets() and self.data_sets_int & ds == ds)

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
        self.subscribe_to_events()
        for data_set in self.data_sets:
            self.task_number += 1
            self.data_set = data_set
            self.report_sync_results()
            self.results[data_set] = self.s3_sync.execute(sync_direction, file_type, data_set, year)
            if self.results[data_set].failure:
                return self.results
            self.spinners[data_set].stop()
        self.report_sync_results()
        self.unsubscribe_from_events()
        return self.results

    def valid_data_sets(self):
        data_set_file_type_map = {
            VigFile.SCRAPED_HTML: list(DataSet),
            VigFile.PARSED_JSON: list(DataSet),
            VigFile.COMBINED_GAME_DATA: [DataSet.ALL],
            VigFile.PATCH_LIST: [
                DataSet.BBREF_GAMES_FOR_DATE,
                DataSet.BBREF_BOXSCORES,
                DataSet.BROOKS_GAMES_FOR_DATE,
                DataSet.BROOKS_PITCHFX,
            ],
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
        src_folder = "S3 bucket" if self.sync_direction == SyncDirection.DOWN_TO_LOCAL else "local folder"
        dest_folder = "local folder" if self.sync_direction == SyncDirection.DOWN_TO_LOCAL else "S3 bucket"
        heading = f"Syncing data from {src_folder} to {dest_folder}"
        print_heading(heading, fg="bright_yellow")

    def error_occurred(self, error_message):
        self.sync_results.append((error_message, SYNC_STATUS_TEXT_COLOR["error"]))
        self.results[self.data_set] = Result.Fail(error_message)

    def get_s3_objects_start(self):
        subprocess.run(["clear"])
        self.spinners["default"] = Halo(
            spinner=get_random_dots_spinner(),
            color=get_random_cli_color(),
            text="Retrieving data for all objects stored in S3...",
        )
        self.spinners["default"].start()

    def get_s3_objects_complete(self):
        self.spinners["default"].stop()

    def find_out_of_sync_files_start(self):
        self.spinners[self.data_set] = Halo(
            spinner=get_random_dots_spinner(),
            color=get_random_cli_color(),
            text=(
                f"Analyzing MLB {self.year} {self.file_type} {self.data_set} files "
                f"(Task {self.task_number}/{self.total_tasks})..."
            ),
        )
        self.spinners[self.data_set].start()

    def find_out_of_sync_files_complete(self, sync_results):
        (out_of_sync, new_files, update_files) = sync_results
        self.sync_files[self.data_set] = (out_of_sync, new_files, update_files)
        sync_files = self.sync_files_text(out_of_sync, new_files, update_files)
        text_color = SYNC_STATUS_TEXT_COLOR["out_of_sync"] if out_of_sync else SYNC_STATUS_TEXT_COLOR["in_sync"]
        self.sync_results.append((sync_files, text_color))

    def sync_files_text(self, out_of_sync, new_files, update_files):
        sync_count = len(new_files) + len(update_files)
        sync_files = f"{sync_count} files out of sync" if out_of_sync else "All files in sync"
        return (
            f"[{self.year} {self.file_type} {self.data_set}] {sync_files} (Task {self.task_number}/{self.total_tasks})"
        )

    def sync_files_start(self, name, complete, total):
        self.report_sync_results()
        self.sync_files_progress(name, complete, total)

    def sync_files_progress(self, name, complete, total):
        direction = "Down" if self.sync_direction == SyncDirection.DOWN_TO_LOCAL else "Up"
        percent = complete / float(total)
        progress_message = f"{direction}loading: {name} | {percent:.0%} ({complete}/{total} Files)"
        self.spinners[self.data_set].text = progress_message

    def sync_files_complete(self):
        dest_folder = "local folder" if self.sync_direction == SyncDirection.DOWN_TO_LOCAL else "s3 bucket"
        src_folder = "s3 bucket" if self.sync_direction == SyncDirection.DOWN_TO_LOCAL else "local folder"
        sync_complete = f"[{self.year} {self.file_type} {self.data_set}] {dest_folder} is in sync with {src_folder}!"
        self.sync_results.append((sync_complete, SYNC_STATUS_TEXT_COLOR["sync_complete"]))

    def subscribe_to_events(self):
        self.s3_sync.events.error_occurred += self.error_occurred
        self.s3_sync.events.get_s3_objects_start += self.get_s3_objects_start
        self.s3_sync.events.get_s3_objects_complete += self.get_s3_objects_complete
        self.s3_sync.events.find_out_of_sync_files_start += self.find_out_of_sync_files_start
        self.s3_sync.events.find_out_of_sync_files_complete += self.find_out_of_sync_files_complete
        self.s3_sync.events.sync_files_start += self.sync_files_start
        self.s3_sync.events.sync_files_progress += self.sync_files_progress
        self.s3_sync.events.sync_files_complete += self.sync_files_complete

    def unsubscribe_from_events(self):
        self.s3_sync.events.error_occurred -= self.error_occurred
        self.s3_sync.events.get_s3_objects_start -= self.get_s3_objects_start
        self.s3_sync.events.get_s3_objects_complete -= self.get_s3_objects_complete
        self.s3_sync.events.find_out_of_sync_files_start -= self.find_out_of_sync_files_start
        self.s3_sync.events.find_out_of_sync_files_complete -= self.find_out_of_sync_files_complete
        self.s3_sync.events.sync_files_start -= self.sync_files_start
        self.s3_sync.events.sync_files_progress -= self.sync_files_progress
        self.s3_sync.events.sync_files_complete -= self.sync_files_complete
