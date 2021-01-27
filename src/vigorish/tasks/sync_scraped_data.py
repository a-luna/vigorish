import subprocess
from copy import deepcopy
from datetime import timezone
from pathlib import Path

from events import Events
from halo import Halo

from vigorish.cli.components import get_random_cli_color, get_random_dots_spinner
from vigorish.enums import DataSet, SyncDirection, VigFile
from vigorish.tasks.base import Task
from vigorish.util.datetime_util import dtaware_fromtimestamp
from vigorish.util.regex import URL_ID_REGEX
from vigorish.util.result import Result

TIME_DIFFERENCE_SECONDS = 60


class SyncScrapedDataTask(Task):
    def __init__(self, app, cached_s3_objects=None):
        super().__init__(app)
        self._cached_s3_objects = cached_s3_objects
        self.sync_direction = None
        self.file_type = None
        self.data_set = None
        self.year = None
        self.spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        self.events = Events(
            (
                "error_occurred",
                "get_sync_files_start",
                "get_sync_files_complete",
                "sync_files_start",
                "sync_files_progress",
                "sync_files_complete",
            )
        )

    @property
    def file_helper(self):
        return self.scraped_data.file_helper

    @property
    def cached_s3_objects(self):
        if self._cached_s3_objects:
            return self._cached_s3_objects
        self._cached_s3_objects = list(self.file_helper.get_all_object_keys_in_s3_bucket())
        return self._cached_s3_objects

    def execute(self, sync_direction, file_type, data_set, year):
        if sync_direction == SyncDirection.UP_TO_S3:
            return self.sync_up_to_s3_bucket(file_type, data_set, year)
        if sync_direction == SyncDirection.DOWN_TO_LOCAL:
            return self.sync_down_to_local_folder(file_type, data_set, year)
        error_message = f"Invalid value for sync_direction: {sync_direction}"
        self.events.error_occurred(error_message)
        return Result.Fail(error_message)

    def sync_up_to_s3_bucket(self, file_type, data_set, year):
        result = self.get_files_to_sync_to_s3(file_type, data_set, year)
        if result.failure:
            self.events.error_occurred("Error occurred analyzing which files need to be synced.")
            return result
        (out_of_sync, missing_files, outdated_files) = result.value
        if not out_of_sync:
            return Result.Ok()
        subprocess.run(["clear"])
        self.upload_files_to_s3(missing_files, outdated_files, file_type, data_set, year)
        return Result.Ok()

    def sync_down_to_local_folder(self, file_type, data_set, year):
        result = self.get_files_to_sync_to_local(file_type, data_set, year)
        if result.failure:
            self.events.error_occurred("Error occurred analyzing which files need to be synced.")
            return result
        (out_of_sync, missing_files, outdated_files) = result.value
        if not out_of_sync:
            return Result.Ok()
        subprocess.run(["clear"])
        self.download_files_to_local_folder(missing_files, outdated_files, file_type, data_set, year)
        return Result.Ok()

    def get_files_to_sync_to_local(self, file_type, data_set, year):
        self.events.get_sync_files_start()
        (s3_objects, local_files) = self.get_all_files_stored_locally_and_in_s3(file_type, data_set, year)
        if not s3_objects:
            sync_results = (False, [], [])
            self.events.get_sync_files_complete(sync_results)
            return Result.Ok(sync_results)
        sync_results = get_files_to_sync(s3_objects, local_files)
        self.events.get_sync_files_complete(sync_results)
        return Result.Ok(sync_results)

    def get_files_to_sync_to_s3(self, file_type, data_set, year):
        self.events.get_sync_files_start()
        (s3_objects, local_files) = self.get_all_files_stored_locally_and_in_s3(file_type, data_set, year)
        if not local_files:
            sync_results = (False, [], [])
            self.events.get_sync_files_complete(sync_results)
            return Result.Ok(sync_results)
        sync_results = get_files_to_sync(local_files, s3_objects)
        self.events.get_sync_files_complete(sync_results)
        return Result.Ok(sync_results)

    def get_all_files_stored_locally_and_in_s3(self, file_type, data_set, year):
        local_files = self.get_all_objects_in_local_folder(file_type, data_set, year)
        s3_objects = self.get_all_objects_in_s3(file_type, data_set, year)
        return (s3_objects, local_files)

    def get_all_objects_in_local_folder(self, file_type, data_set, year):
        folderpath = self.scraped_data.get_local_folderpath(file_type, data_set, year)
        local_files = [
            get_local_file_data(file)
            for file in Path(folderpath).glob("*.*")
            if URL_ID_REGEX[file_type][data_set].search(file.stem)
        ]
        return sorted(local_files, key=lambda x: x["name"]) if local_files else []

    def get_all_objects_in_s3(self, file_type, data_set, year):
        s3_objects_dict = {
            VigFile.SCRAPED_HTML: self.get_all_html_objects_in_s3,
            VigFile.PARSED_JSON: self.get_all_json_objects_in_s3,
            VigFile.COMBINED_GAME_DATA: self.get_all_combined_data_objects_in_s3,
            VigFile.PATCH_LIST: self.get_all_patch_list_objects_in_s3,
        }
        return s3_objects_dict[file_type](data_set, year) if file_type in s3_objects_dict else []

    def get_all_html_objects_in_s3(self, data_set, year):
        html_folder = self.scraped_data.get_s3_folderpath(VigFile.SCRAPED_HTML, data_set, year)
        s3_objects = [
            get_s3_object_data(obj)
            for obj in self.cached_s3_objects
            if html_folder in obj.key and URL_ID_REGEX[VigFile.SCRAPED_HTML][data_set].search(Path(obj.key).stem)
        ]
        return sorted(s3_objects, key=lambda x: x["name"]) if s3_objects else []

    def get_all_json_objects_in_s3(self, data_set, year):
        json_folder = self.scraped_data.get_s3_folderpath(VigFile.PARSED_JSON, data_set, year)
        html_folder = self.scraped_data.get_s3_folderpath(VigFile.SCRAPED_HTML, data_set, year)
        s3_objects = [
            get_s3_object_data(obj)
            for obj in self.cached_s3_objects
            if json_folder in obj.key
            and html_folder not in obj.key
            and URL_ID_REGEX[VigFile.PARSED_JSON][data_set].search(Path(obj.key).stem)
        ]
        return sorted(s3_objects, key=lambda x: x["name"]) if s3_objects else []

    def get_all_combined_data_objects_in_s3(self, data_set, year):
        comb_folder = self.scraped_data.get_s3_folderpath(VigFile.COMBINED_GAME_DATA, DataSet.ALL, year)
        s3_objects = [
            get_s3_object_data(obj)
            for obj in self.cached_s3_objects
            if comb_folder in obj.key
            and URL_ID_REGEX[VigFile.COMBINED_GAME_DATA][DataSet.ALL].search(Path(obj.key).stem)
        ]
        return sorted(s3_objects, key=lambda x: x["name"]) if s3_objects else []

    def get_all_patch_list_objects_in_s3(self, data_set, year):
        json_folder = self.scraped_data.get_s3_folderpath(VigFile.PARSED_JSON, data_set, year)
        s3_objects = [
            get_s3_object_data(obj)
            for obj in self.cached_s3_objects
            if json_folder in obj.key and URL_ID_REGEX[VigFile.PATCH_LIST][data_set].search(Path(obj.key).stem)
        ]
        return sorted(s3_objects, key=lambda x: x["name"]) if s3_objects else []

    def upload_files_to_s3(self, missing_files, outdated_files, file_type, data_set, year):
        sync_files = get_sync_files(missing_files, outdated_files)
        self.sync_files(SyncDirection.UP_TO_S3, sync_files, file_type, data_set, year)

    def download_files_to_local_folder(self, missing_files, outdated_files, file_type, data_set, year):
        sync_files = get_sync_files(missing_files, outdated_files)
        self.sync_files(SyncDirection.DOWN_TO_LOCAL, sync_files, file_type, data_set, year)

    def sync_files(self, sync_direction, files, file_type, data_set, year):
        self.file_helper.create_all_folderpaths(year)
        self.bucket_name = self.config.get_current_setting("S3_BUCKET", data_set)
        self.events.sync_files_start(files[0]["name"], 0, len(files))
        for num, file in enumerate(files, start=1):
            self.events.sync_files_progress(file["name"], num - 1, len(files))
            (filepath, s3_key) = self.get_local_path_and_s3_key(file, file_type, data_set, year)
            self.send_file(sync_direction, filepath, s3_key)
            self.events.sync_files_progress(file["name"], num, len(files))
        self.events.sync_files_complete()

    def get_local_path_and_s3_key(self, file, file_type, data_set, year):
        local_folder = self.scraped_data.get_local_folderpath(file_type, data_set, year)
        s3_folder = self.scraped_data.get_s3_folderpath(file_type, data_set, year)
        local_path = str(Path(local_folder).joinpath(file["name"]))
        s3_key = f'{s3_folder}/{file["name"]}'
        return (local_path, s3_key)

    def send_file(self, sync_direction, local_path, s3_key):
        if sync_direction == SyncDirection.UP_TO_S3:
            self.file_helper.get_s3_bucket().upload_file(local_path, s3_key)
        if sync_direction == SyncDirection.DOWN_TO_LOCAL:
            self.file_helper.get_s3_bucket().download_file(s3_key, local_path)


def get_local_file_data(file):
    return {
        "name": file.name,
        "path": file,
        "size": file.stat().st_size,
        "last_modified": dtaware_fromtimestamp(file.stat().st_mtime, use_tz=timezone.utc),
    }


def get_s3_object_data(obj):
    return {
        "name": Path(obj.key).name,
        "path": Path(obj.key),
        "size": obj.size,
        "last_modified": obj.last_modified,
    }


def get_files_to_sync(src_files, dest_files):
    missing_files = find_missing_files(src_files, dest_files)
    outdated_files = find_outdated_files(src_files, dest_files)
    if missing_files or outdated_files:
        return (True, missing_files, outdated_files)
    return (False, [], [])


def find_missing_files(src_files, dest_files):
    dest_names = [f["name"] for f in dest_files if f["size"] > 0]
    return list(map(add_new_file, filter(lambda x: x["name"] not in dest_names, src_files)))


def add_new_file(file):
    file["operation"] = "ADD NEW FILE"
    return file


def find_outdated_files(src_files, dest_files):
    common_files = find_common_files(src_files, dest_files)
    return list(map(update_existing_file, filter(src_file_is_newer, common_files)))


def find_common_files(src_files, dest_files):
    src_names = {f["name"] for f in src_files if f["size"] > 0}
    dest_names = [f["name"] for f in dest_files if f["size"] > 0]
    common_names = src_names.intersection(dest_names)
    src_common_files = sorted(filter(lambda x: x["name"] in common_names, src_files), key=lambda x: x["name"])
    dest_common_files = sorted(filter(lambda x: x["name"] in common_names, dest_files), key=lambda x: x["name"])
    return zip(src_common_files, dest_common_files)


def src_file_is_newer(file_pair):
    (src, dest) = file_pair
    diff = src["last_modified"] - dest["last_modified"]
    return diff.total_seconds() > TIME_DIFFERENCE_SECONDS and src["size"] != dest["size"]


def update_existing_file(file_pair):
    (src, _) = file_pair
    src["operation"] = "UPDATE EXISTING FILE"
    return src


def get_sync_files(missing_files, outdated_files):
    sync_files = []
    if missing_files:
        sync_files = deepcopy(missing_files)
    if outdated_files:
        sync_files.extend(deepcopy(outdated_files))
    return sync_files
