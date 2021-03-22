"""Menu item that returns the user to the previous menu."""
import subprocess
from collections import defaultdict
from copy import deepcopy

from getch import pause
from halo import Halo

from vigorish.cli.components import (
    data_sets_prompt,
    file_types_prompt,
    get_random_cli_color,
    get_random_dots_spinner,
    print_heading,
    print_message,
    season_prompt,
    user_options_prompt,
)
from vigorish.cli.components.viewers import DictListTableViewer
from vigorish.cli.menu_item import MenuItem
from vigorish.constants import EMOJIS, MENU_NUMBERS
from vigorish.enums import DataSet, SyncDirection, VigFile
from vigorish.tasks.sync_scraped_data import SyncScrapedDataTask
from vigorish.util.dt_format_strings import DT_NAIVE_LESS
from vigorish.util.result import Result
from vigorish.util.sys_helpers import file_size_str

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
        self.menu_item_emoji = EMOJIS.get("CLOUD", "")
        self.menu_heading = self.menu_item_text
        self.exit_menu = False

    @property
    def total_tasks(self):
        return sum(len(data_sets) for data_sets in self.sync_tasks.values())

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
        self.get_all_s3_objects()
        for file_type, data_sets in self.sync_tasks.items():
            for data_set in data_sets:
                result = self.find_out_of_sync_files(file_type, data_set)
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
        heading = self.get_menu_heading(f"Select Data Sets to Sync for {file_type}")
        prompt = f"Select all data sets to synchronize for {file_type} files"
        if file_type == VigFile.COMBINED_GAME_DATA:
            return [DataSet.ALL]
        valid_data_sets = (
            [DataSet.ALL]
            if file_type != VigFile.PATCH_LIST
            else [
                DataSet.BBREF_GAMES_FOR_DATE,
                DataSet.BBREF_BOXSCORES,
                DataSet.BROOKS_GAMES_FOR_DATE,
                DataSet.BROOKS_PITCHFX,
            ]
        )
        return data_sets_prompt(heading, prompt, valid_data_sets)

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
            f"{EMOJIS.get('BACK')} Return to Main Menu": None,
        }
        return user_options_prompt(choices, prompt)

    def get_all_s3_objects(self):
        subprocess.run(["clear"])
        spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        spinner.text = "Retrieving details of all objects stored in S3..."
        spinner.start()
        self.s3_sync.get_all_s3_objects()
        spinner.stop()

    def find_out_of_sync_files(self, file_type, data_set):
        self.task_number += 1
        self.report_sync_results()
        self.update_spinner(file_type, data_set)
        result = Result.Fail("Failed to sync files")
        result = self.s3_sync.find_out_of_sync_files(self.sync_direction, file_type, data_set, self.year)
        if result.failure:
            self.spinners[file_type][data_set].stop()
            return result
        (out_of_sync, missing_files, update_files) = result.value
        self.sync_files[file_type][data_set] = (out_of_sync, missing_files, update_files)
        if out_of_sync:
            self.sync_results.append(self.out_of_sync_text(file_type, data_set, len(missing_files) + len(update_files)))
        else:
            self.sync_results.append(self.in_sync_text(file_type, data_set))
        self.spinners[file_type][data_set].stop()
        return Result.Ok()

    def report_sync_results(self):
        subprocess.run(["clear"])
        src_folder = "S3 bucket" if self.sync_direction == SyncDirection.DOWN_TO_LOCAL else "local folder"
        dest_folder = "local folder" if self.sync_direction == SyncDirection.DOWN_TO_LOCAL else "S3 bucket"
        print_heading(f"Syncing data from {src_folder} to {dest_folder}", fg="bright_yellow")
        for (result, text_color) in self.sync_results:
            print_message(result, fg=text_color)

    def update_spinner(self, file_type, data_set):
        self.spinners[file_type][data_set] = Halo(
            spinner=get_random_dots_spinner(),
            color=get_random_cli_color(),
            text=(
                f"Analyzing MLB {self.year} {data_set} {file_type} files "
                f"(Task {self.task_number}/{self.total_tasks})..."
            ),
        )
        self.spinners[file_type][data_set].start()

    def out_of_sync_text(self, file_type, data_set, sync_count):
        out_of_sync = (
            f"{sync_count} files out of sync: {self.year} {data_set} {file_type} "
            f"(Task {self.task_number}/{self.total_tasks})"
        )
        return (out_of_sync, SYNC_STATUS_TEXT_COLOR["out_of_sync"])

    def in_sync_text(self, file_type, data_set):
        in_sync = f"All files in sync: {self.year} {data_set} {file_type} (Task {self.task_number}/{self.total_tasks})"
        return (in_sync, SYNC_STATUS_TEXT_COLOR["in_sync"])

    def synchronize_files(self):
        if self.all_files_are_in_sync:
            message = "All files for selected data sets are in sync!"
            print_message(message, fg="bright_green", bold=True)
            pause(message="Press any key to continue...")
            return Result.Ok()
        for file_type, file_type_dict in self.sync_files.items():
            for data_set, (out_of_sync, missing_files, outdated_files) in file_type_dict.items():
                if not out_of_sync:
                    continue
                all_sync_files = []
                missing_count = 0
                outdated_count = 0
                if missing_files:
                    all_sync_files.extend(missing_files)
                    missing_count = len(missing_files)
                if outdated_files:
                    all_sync_files.extend(outdated_files)
                    outdated_count = len(outdated_files)
                table_viewer = self.create_table_viewer(
                    all_sync_files, data_set, file_type, missing_count, outdated_count
                )
                apply_changes = table_viewer.launch()
                if apply_changes:
                    self.apply_pending_changes(file_type, data_set, missing_files, outdated_files)
        return Result.Ok()

    def create_table_viewer(self, sync_files, data_set, file_type, missing_count, outdated_count):
        dict_list = [
            {
                "filename": f["name"],
                "size": file_size_str(f["size"]),
                "last_modified": f'{f["last_modified"].strftime(DT_NAIVE_LESS)} UTC',
                "sync_operation": f["operation"],
            }
            for f in sync_files
        ]
        m = []
        if missing_count:
            missing_plural = "files below do" if missing_count > 1 else "file below does"
            file_dest = "S3 bucket" if self.sync_direction == SyncDirection.UP_TO_S3 else "local folder"
            m.append(f"{missing_count} {missing_plural} not exist in the {file_dest}")
        if outdated_count:
            outdated_plural = "files have" if outdated_count > 1 else "file has"
            file_src = "local folder" if self.sync_direction == SyncDirection.UP_TO_S3 else "S3 bucket"
            m.append(f"{outdated_count} {outdated_plural} a more recent version in the {file_src}")
        return DictListTableViewer(
            dict_list,
            prompt="Would you like to apply the changes to the files listed above?",
            heading=f"File sync changes for {file_type} Files (Data Set: {data_set})",
            heading_color="bright_yellow",
            message="\n".join(m),
            table_color="bright_cyan",
        )

    def apply_pending_changes(self, file_type, data_set, missing_files, outdated_files):
        subprocess.run(["clear"])
        self.spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        self.spinner.start()
        self.s3_sync.events.sync_files_progress += self.update_sync_progress
        self.s3_sync.sync_files(self.sync_direction, missing_files, outdated_files, file_type, data_set, self.year)
        self.s3_sync.events.sync_files_progress -= self.update_sync_progress
        self.spinner.stop()
        src_folder = "S3 bucket" if self.sync_direction == SyncDirection.DOWN_TO_LOCAL else "local folder"
        dest_folder = "local folder" if self.sync_direction == SyncDirection.DOWN_TO_LOCAL else "S3 bucket"
        message = (
            f"All changes have been applied, MLB {self.year} {data_set} {file_type} files in {src_folder} "
            f"have been synced to {dest_folder}!"
        )
        print_message(message, fg="bright_green", bold=True)
        pause(message="Press any key to continue...")

    def get_sync_files(self, missing_files, outdated_files):
        sync_files = []
        if not missing_files and not outdated_files:
            return sync_files
        if missing_files:
            sync_files = deepcopy(missing_files)
        if outdated_files:
            sync_files.extend(deepcopy(outdated_files))
        return sync_files

    def update_sync_progress(self, name, complete, total):
        direction = "Down" if self.sync_direction == SyncDirection.DOWN_TO_LOCAL else "Up"
        percent = complete / float(total)
        self.spinner.text = f"{direction}loading {name} {percent:.0%} ({complete}/{total} Files)"
