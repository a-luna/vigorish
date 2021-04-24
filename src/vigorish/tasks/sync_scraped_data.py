from copy import deepcopy
from functools import cached_property
from pathlib import Path

from events import Events
from halo import Halo

from vigorish.cli.components import get_random_cli_color, get_random_dots_spinner
from vigorish.enums import SyncDirection, VigFile
from vigorish.tasks.base import Task
from vigorish.util.regex import URL_ID_REGEX
from vigorish.util.result import Result
from vigorish.util.sys_helpers import get_last_mod_time_utc

TIME_DIFFERENCE_SECONDS = 60


class SyncScrapedDataTask(Task):
    def __init__(self, app):
        super().__init__(app)
        self.sync_direction = None
        self.file_type = None
        self.data_set = None
        self.year = None
        self.spinner = Halo(spinner=get_random_dots_spinner(), color=get_random_cli_color())
        self.events = Events(
            (
                "error_occurred",
                "get_s3_objects_start",
                "get_s3_objects_complete",
                "find_out_of_sync_files_start",
                "find_out_of_sync_files_complete",
                "sync_files_start",
                "sync_files_progress",
                "sync_files_complete",
            )
        )

    @property
    def file_helper(self):
        return self.scraped_data.file_helper

    @cached_property
    def all_s3_objects(self):  # pragma: no cover
        return self.file_helper.get_all_object_keys_in_s3_bucket()

    def execute(self, sync_direction, file_type, data_set, year):
        self.get_all_s3_objects()
        result = self.find_out_of_sync_files(sync_direction, file_type, data_set, year)
        if result.failure:
            self.events.error_occurred("Error occurred analyzing which files need to be synced.")
            return result
        (out_of_sync, missing_files, outdated_files) = result.value
        if not out_of_sync:
            return Result.Ok()
        self.sync_files(sync_direction, missing_files, outdated_files, file_type, data_set, year)
        return Result.Ok()

    def get_all_s3_objects(self):
        self.events.get_s3_objects_start()
        self.all_s3_objects
        self.events.get_s3_objects_complete()

    def find_out_of_sync_files(self, sync_direction, file_type, data_set, year):
        self.events.find_out_of_sync_files_start()
        (s3_objects, local_files) = self.get_all_files_in_src_and_dest(file_type, data_set, year)
        if (sync_direction == SyncDirection.UP_TO_S3 and not local_files) or (
            sync_direction == SyncDirection.DOWN_TO_LOCAL and not s3_objects
        ):
            sync_results = (False, [], [])
            self.events.find_out_of_sync_files_complete(sync_results)
            return Result.Ok(sync_results)
        src_files = local_files if sync_direction == SyncDirection.UP_TO_S3 else s3_objects
        dest_files = s3_objects if sync_direction == SyncDirection.UP_TO_S3 else local_files
        sync_results = get_files_to_sync(src_files, dest_files)
        self.events.find_out_of_sync_files_complete(sync_results)
        return Result.Ok(sync_results)

    def get_all_files_in_src_and_dest(self, file_type, data_set, year):
        local_files = self.get_local_files(file_type, data_set, year)
        s3_objects = self.get_s3_objects(file_type, data_set, year)
        return (s3_objects, local_files)

    def get_local_files(self, file_type, data_set, year):
        folderpath = self.scraped_data.get_local_folderpath(file_type, data_set, year)
        id_regex = URL_ID_REGEX[file_type][data_set]
        local_files = filter(lambda x: id_regex.search(x.stem), Path(folderpath).glob("*.*"))
        return sorted(map(get_local_file_data, local_files), key=lambda x: x["name"])

    def get_s3_objects(self, file_type, data_set, year):
        folderpath = self.scraped_data.get_s3_folderpath(file_type, data_set, year)
        id_regex = URL_ID_REGEX[file_type][data_set]
        file_suffix = ".html" if file_type == VigFile.SCRAPED_HTML else ".json"
        s3_objects = filter(
            lambda x: folderpath in x.key and id_regex.search(Path(x.key).stem) and Path(x.key).suffix == file_suffix,
            self.all_s3_objects,
        )
        return sorted(map(get_s3_object_data, s3_objects), key=lambda x: x["name"])

    def sync_files(self, sync_direction, missing_files, outdated_files, file_type, data_set, year):
        self.file_helper.create_all_folderpaths(year)
        self.bucket_name = self.config.get_current_setting("S3_BUCKET", data_set)
        sync_files = get_sync_files(missing_files, outdated_files)
        self.events.sync_files_start(sync_files[0]["name"], 0, len(sync_files))
        for num, file in enumerate(sync_files, start=1):
            self.events.sync_files_progress(file["name"], num - 1, len(sync_files))
            (filepath, s3_key) = self.get_local_path_and_s3_key(file, file_type, data_set, year)
            self.send_file(sync_direction, filepath, s3_key)
            self.events.sync_files_progress(file["name"], num, len(sync_files))
        self.events.sync_files_complete()

    def get_local_path_and_s3_key(self, file, file_type, data_set, year):
        local_folder = self.scraped_data.get_local_folderpath(file_type, data_set, year)
        s3_folder = self.scraped_data.get_s3_folderpath(file_type, data_set, year)
        local_path = str(Path(local_folder).joinpath(file["name"]))
        s3_key = f'{s3_folder}/{file["name"]}'
        return (local_path, s3_key)

    def send_file(self, sync_direction, local_path, s3_key):  # pragma: no cover
        if sync_direction == SyncDirection.UP_TO_S3:
            self.file_helper.get_s3_bucket().upload_file(local_path, s3_key)
        if sync_direction == SyncDirection.DOWN_TO_LOCAL:
            self.file_helper.get_s3_bucket().download_file(s3_key, local_path)


def get_local_file_data(file):
    return {
        "name": file.name,
        "path": file,
        "size": file.stat().st_size,
        "last_modified": get_last_mod_time_utc(file),
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
