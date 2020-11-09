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
    print_message,
    season_prompt,
    user_options_prompt,
)
from vigorish.cli.components.dict_viewer import DictListTableViewer
from vigorish.cli.menu_item import MenuItem
from vigorish.constants import EMOJI_DICT, MENU_NUMBERS
from vigorish.enums import DataSet, SyncDirection, VigFile
from vigorish.tasks.sync_scraped_data import SyncScrapedData as SyncScrapedDataTask
from vigorish.util.dt_format_strings import DT_AWARE
from vigorish.util.result import Result

SYNC_STATUS_TEXT_COLOR = {"out_of_sync": "bright_green", "in_sync": "blue"}


class SyncScrapedData(MenuItem):
    def __init__(self, app):
        super().__init__(app)
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
        self.initialize_s3_sync_task()
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

    def initialize_s3_sync_task(self):
        subprocess.run(["clear"])
        spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        spinner.text = "Retrieving details of all objects stored in S3..."
        spinner.start()
        s3_obj_collection = self.scraped_data.file_helper.get_all_object_keys_in_s3_bucket()
        s3_objects = [obj for obj in s3_obj_collection]
        self.s3_sync = SyncScrapedDataTask(self.app, cached_s3_objects=s3_objects)
        spinner.stop()

    def get_files_to_sync(self, file_type, data_set):
        self.task_number += 1
        self.report_sync_results()
        self.update_spinner(file_type, data_set)
        result = Result.Fail("Failed to sync files")
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
            for data_set, (out_of_sync, new_files, old_files) in file_type_dict.items():
                if not out_of_sync:
                    continue
                all_sync_files = []
                new_count = 0
                old_count = 0
                if new_files:
                    all_sync_files.extend(new_files)
                    new_count = len(new_files)
                if old_files:
                    all_sync_files.extend(old_files)
                    old_count = len(old_files)
                table_viewer = self.create_table_viewer(
                    all_sync_files, data_set, file_type, new_count, old_count
                )
                apply_changes = table_viewer.launch()
                if apply_changes:
                    self.apply_pending_changes(file_type, data_set, new_files, old_files)
        return Result.Ok()

    def create_table_viewer(self, sync_files, data_set, file_type, new_count, old_count):
        dict_list = [
            {
                "filename": f["name"],
                "size": f["size"],
                "last_modified": f["last_modified"].strftime(DT_AWARE),
                "sync_operation": f["operation"],
            }
            for f in sync_files
        ]
        new_plural = "files below do" if new_count > 1 else "file below does"
        old_plural = "files have" if old_count > 1 else "file has"
        file_dest = "S3 bucket" if self.sync_direction == SyncDirection.UP_TO_S3 else "local folder"
        file_src = "local folder" if self.sync_direction == SyncDirection.UP_TO_S3 else "S3 bucket"
        m = []
        if new_count:
            m.append(f"{new_count} {new_plural} not exist in the {file_dest}")
        if old_count:
            m.append(f"{old_count} {old_plural} a more recent version in the {file_src}")
        return DictListTableViewer(
            dict_list,
            prompt="Would you like to apply the changes to the files listed above?",
            heading=f"File sync changes for {file_type} Files (Data Set: {data_set})",
            heading_color="bright_yellow",
            message="\n".join(m),
            table_color="bright_cyan",
        )

    def apply_pending_changes(self, file_type, data_set, new_files, old_files):
        subprocess.run(["clear"])
        self.s3_sync.events.sync_files_progress += self.update_sync_progress
        self.spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        self.spinner.start()
        if self.sync_direction == SyncDirection.UP_TO_S3:
            self.s3_sync.upload_files_to_s3(new_files, old_files, file_type, data_set, self.year)
            message = (
                f"All changes have been applied, MLB {self.year} {data_set} {file_type} files "
                "in local folder have been synced to s3 bucket!"
            )
        if self.sync_direction == SyncDirection.DOWN_TO_LOCAL:
            self.s3_sync.download_files_to_local_folder(
                new_files, old_files, file_type, data_set, self.year
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
        self.spinner.text = f"{direction}loading {name} {percent:.0%} ({complete}/{total} Files)"
