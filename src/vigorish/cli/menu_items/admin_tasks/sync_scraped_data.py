"""Menu item that returns the user to the previous menu."""
import subprocess
from collections import defaultdict
from copy import deepcopy

from getch import pause
from halo import Halo
from tabulate import tabulate

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.prompts import (
    season_prompt,
    data_sets_prompt,
    user_options_prompt,
    prompt_user_yes_no,
    file_types_prompt,
)
from vigorish.cli.util import (
    print_message,
    get_random_bright_cli_color,
    get_random_cli_color,
    get_random_dots_spinner,
)
from vigorish.constants import EMOJI_DICT, MENU_NUMBERS
from vigorish.tasks.sync_scraped_data import SyncScrapedData as SyncScrapedDataTask
from vigorish.enums import VigFile, DataSet, SyncDirection
from vigorish.util.dt_format_strings import DT_AWARE
from vigorish.util.result import Result

SYNC_STATUS_TEXT_COLOR = {"out_of_sync": "bright_green", "in_sync": "blue"}


class SyncScrapedData(MenuItem):
    def __init__(self, app):
        super().__init__(app)
        self.s3_sync = SyncScrapedDataTask(self.app)
        self.year = None
        self.sync_direction = ""
        self.file_types = []
        self.sync_tasks = defaultdict(list)
        self.sync_files = defaultdict(dict)
        self.sync_results = []
        self.task_number = 0
        self.spinners = defaultdict(dict)
        self.menu_item_text = " Synchronize Scraped Data"
        self.menu_item_emoji = EMOJI_DICT.get("CLOUD", "")
        self.exit_menu = False

    @property
    def total_tasks(self):
        return sum([len(data_sets) for data_sets in self.sync_tasks.values()])

    @property
    def all_files_are_in_sync(self):
        return all(
            not out_of_sync
            for doc_format_dict in self.sync_files.values()
            for (out_of_sync, _, _) in doc_format_dict.values()
        )

    def launch(self):
        result = self.get_sync_parameters()
        if result.failure:
            return Result.Ok(False)
        for file_type, data_sets in self.sync_tasks.items():
            for data_set in data_sets:
                result = self.get_files_to_sync(file_type, data_set)
                if result.failure:
                    return result
        self.report_sync_results()
        return self.synchronize_files()

    def get_sync_parameters(self):
        result = season_prompt(self.db_session, "Select a season to synchronize scraped data:")
        if result.failure:
            return Result.Fail("")
        self.year = result.value.year
        self.file_types = file_types_prompt("Select one or multiple file types to synchronize:")
        for file_type in self.file_types:
            self.sync_tasks[file_type] = self.get_data_sets_to_sync(file_type)
        result = self.sync_direction_prompt()
        if result.failure:
            return Result.Fail("")
        self.sync_direction = result.value
        return Result.Ok()

    def get_data_sets_to_sync(self, file_type):
        prompt = f"Select Data Sets to Synchronize for File Type: {file_type}"
        if file_type == VigFile.COMBINED_GAME_DATA:
            return [DataSet.ALL]
        if file_type == VigFile.PATCH_LIST:
            valid_data_sets = DataSet.BBREF_BOXSCORES | DataSet.BROOKS_PITCHFX
        else:
            valid_data_sets = DataSet.ALL
        return data_sets_prompt(prompt, valid_data_sets=int(valid_data_sets))

    def sync_direction_prompt(self):
        prompt = (
            'Please choose the "direction" of the sync operation. Files in the source location '
            "are compared to files in the destination, if a source file does not exist in the "
            "destination, it will be copied to the destination. Files that exist in both "
            "locations are compared, if the destination file is older it will be replaced by "
            "the updated version from the source location. Files are never deleted from either "
            "location during a sync."
        )
        SYNC_UP_MENU_CHOICE = "Sync Up: Local Folder (Source) -> S3 Bucket (Dest)"
        SYNC_DOWN_MENU_CHOICE = "Sync Down: S3 Bucket (Source) -> Local Folder (Dest)"
        choices = {
            f"{MENU_NUMBERS.get(1)}  {SYNC_UP_MENU_CHOICE}": SyncDirection.UP_TO_S3,
            f"{MENU_NUMBERS.get(2)}  {SYNC_DOWN_MENU_CHOICE}": SyncDirection.DOWN_TO_LOCAL,
            f"{EMOJI_DICT.get('BACK')} Return to Main Menu": None,
        }
        return user_options_prompt(choices, prompt)

    def get_files_to_sync(self, file_type, data_set):
        self.task_number += 1
        self.report_sync_results()
        self.update_spinner(file_type, data_set)
        if self.sync_direction == SyncDirection.UP_TO_S3:
            result = self.s3_sync.get_files_to_sync_to_s3(file_type, data_set, self.year)
        if self.sync_direction == SyncDirection.DOWN_TO_LOCAL:
            result = self.s3_sync.get_files_to_sync_to_local(file_type, data_set, self.year)
        if result.failure:
            self.spinners[file_type][data_set].stop()
            return result
        (out_of_sync, new_files, update_files) = result.value
        self.sync_files[file_type][data_set] = (out_of_sync, new_files, update_files)
        if out_of_sync:
            sync_count = len(new_files) + len(update_files)
            self.log_data_set_out_of_sync(file_type, data_set, sync_count)
        else:
            self.log_data_set_in_sync(file_type, data_set)
        self.spinners[file_type][data_set].stop()
        return Result.Ok()

    def report_sync_results(self):
        subprocess.run(["clear"])
        if self.sync_direction == SyncDirection.UP_TO_S3:
            heading = "Syncing data from local folder to S3 bucket\n"
        if self.sync_direction == SyncDirection.DOWN_TO_LOCAL:
            heading = "Syncing data from S3 bucket to local folder\n"
        print_message(heading, fg="bright_yellow", wrap=False, bold=True, underline=True)
        if not self.sync_results:
            return
        for task_result in self.sync_results:
            print_message(task_result[0], fg=task_result[1])

    def update_spinner(self, file_type, data_set):
        self.spinners[file_type][data_set] = Halo(spinner=get_random_dots_spinner())
        self.spinners[file_type][data_set].text = self.sync_started_text(file_type, data_set)
        self.spinners[file_type][data_set].color = get_random_cli_color()
        self.spinners[file_type][data_set].start()

    def log_data_set_out_of_sync(self, file_type, data_set, sync_count):
        self.sync_results.append(self.out_of_sync_text(file_type, data_set, sync_count))

    def log_data_set_in_sync(self, file_type, data_set):
        self.sync_results.append(self.in_sync_text(file_type, data_set))

    def sync_started_text(self, file_type, data_set):
        return (
            f"Analyzing MLB {self.year} {data_set} {file_type} files "
            f"(Task {self.task_number}/{self.total_tasks})..."
        )

    def out_of_sync_text(self, file_type, data_set, sync_count):
        out_of_sync = (
            f"{sync_count} files out of sync: {self.year} {data_set} {file_type} "
            f"(Task {self.task_number}/{self.total_tasks})"
        )
        return (out_of_sync, SYNC_STATUS_TEXT_COLOR["out_of_sync"])

    def in_sync_text(self, file_type, data_set):
        in_sync = (
            f"All files in sync: {self.year} {data_set} {file_type} "
            f"(Task {self.task_number}/{self.total_tasks})"
        )
        return (in_sync, SYNC_STATUS_TEXT_COLOR["in_sync"])

    def synchronize_files(self):
        if self.all_files_are_in_sync:
            message = "All files for selected data sets are in sync!"
            print_message(message, fg="bright_green", bold=True)
            pause(message="Press any key to continue...")
            return Result.Ok()
        for file_type, file_type_dict in self.sync_files.items():
            for data_set, (out_of_sync, new_files, update_files) in file_type_dict.items():
                if not out_of_sync:
                    continue
                subprocess.run(["clear"])
                header = f"Sync changes for {data_set} ({file_type} Files)\n"
                print_message(header, bold=True, underline=True)
                if new_files:
                    self.show_files_to_add(new_files)
                if update_files:
                    self.show_files_to_update(update_files)
                if self.prompt_user_sync_files():
                    self.apply_pending_changes(file_type, data_set, new_files, update_files)
        return Result.Ok()

    def show_files_to_add(self, sync_files):
        files_plural = "files below are" if len(sync_files) > 1 else "file below is"
        message = f"{len(sync_files)} {files_plural} not present in the source folder/bucket:\n"
        self.show_out_of_sync_files(sync_files, message, color="bright_yellow")

    def show_files_to_update(self, sync_files):
        files_plural = "files" if len(sync_files) > 1 else "file"
        message = (
            f"\n{len(sync_files)} {files_plural} below will be updated to the most recent "
            "version:\n"
        )
        self.show_out_of_sync_files(sync_files, message, color="bright_cyan")

    def show_out_of_sync_files(self, sync_files, message, color=None):
        color = color or get_random_bright_cli_color()
        display_files = [
            {
                "filename": f["name"],
                "size": f["size"],
                "last_modified": f["last_modified"].strftime(DT_AWARE),
                "sync_operation": f["operation"],
            }
            for f in sync_files
        ]
        print_message(message, fg=color, bold=True)
        print_message(tabulate(display_files, headers="keys"), wrap=False, fg=color)

    def prompt_user_sync_files(self):
        prompt = "\nWould you like to apply the changes to the files listed above?"
        return prompt_user_yes_no(prompt)

    def apply_pending_changes(self, file_type, data_set, new_files, update_files):
        subprocess.run(["clear"])
        self.s3_sync.events.sync_files_progress += self.update_sync_progress
        self.spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        self.spinner.start()
        if self.sync_direction == SyncDirection.UP_TO_S3:
            self.s3_sync.upload_files_to_s3(
                new_files, update_files, file_type, data_set, self.year
            )
            message = (
                f"All changes have been applied, MLB {self.year} {data_set} {file_type} files "
                "in local folder have been synced to s3 bucket!"
            )
        if self.sync_direction == SyncDirection.DOWN_TO_LOCAL:
            self.s3_sync.download_files_to_local_folder(
                new_files, update_files, file_type, data_set, self.year
            )
            message = (
                f"All changes have been applied, MLB {self.year} {data_set} {file_type} files "
                "in s3 bucket have been synced to local folder!"
            )
        self.s3_sync.events.sync_files_progress -= self.update_sync_progress
        self.spinner.stop()
        print_message(message, fg="bright_green", bold=True)
        pause(message="Press any key to continue...")

    def get_sync_files(self, new_files, update_files):
        sync_files = []
        if not new_files and not update_files:
            return sync_files
        if new_files:
            sync_files = deepcopy(new_files)
        if update_files:
            sync_files.extend(deepcopy(update_files))
        return sync_files

    def update_sync_progress(self, name, complete, total):
        direction = "Down" if self.sync_direction == SyncDirection.DOWN_TO_LOCAL else "Up"
        percent = complete / float(total)
        self.spinner.text = f"{direction}loading {name} {percent:.0%} ({complete}/{total}) Files"
