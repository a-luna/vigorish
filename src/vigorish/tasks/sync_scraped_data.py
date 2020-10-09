import subprocess
from collections import Counter
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


class SyncScrapedData(Task):
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
    def s3_folderpath_dict(self):
        return self.scraped_data.file_helper.s3_folderpath_dict

    @property
    def cached_s3_objects(self):
        if self._cached_s3_objects:
            return self._cached_s3_objects
        s3_objects = self.file_helper.get_all_object_keys_in_s3_bucket()
        self._cached_s3_objects = [obj for obj in s3_objects]
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
        (out_of_sync, new_files, update_files) = result.value
        if not out_of_sync:
            return Result.Ok()
        subprocess.run(["clear"])
        self.upload_files_to_s3(new_files, update_files, file_type, data_set, year)
        return Result.Ok()

    def sync_down_to_local_folder(self, file_type, data_set, year):
        result = self.get_files_to_sync_to_local(file_type, data_set, year)
        if result.failure:
            self.events.error_occurred("Error occurred analyzing which files need to be synced.")
            return result
        (out_of_sync, new_files, update_files) = result.value
        if not out_of_sync:
            return Result.Ok()
        subprocess.run(["clear"])
        self.download_files_to_local_folder(new_files, update_files, file_type, data_set, year)
        return Result.Ok()

    def get_files_to_sync_to_local(self, file_type, data_set, year):
        self.events.get_sync_files_start()
        result = self.get_all_files_stored_locally_and_in_s3(file_type, data_set, year)
        if result.failure:
            return result
        (s3_objects, local_files) = result.value
        if not s3_objects:
            sync_results = (False, [], [])
            self.events.get_sync_files_complete(sync_results)
            return Result.Ok(sync_results)
        sync_results = self.get_files_to_sync(s3_objects, local_files)
        self.events.get_sync_files_complete(sync_results)
        return Result.Ok(sync_results)

    def get_files_to_sync_to_s3(self, file_type, data_set, year):
        self.events.get_sync_files_start()
        result = self.get_all_files_stored_locally_and_in_s3(file_type, data_set, year)
        if result.failure:
            return result
        (s3_objects, local_files) = result.value
        if not local_files:
            sync_results = (False, [], [])
            self.events.get_sync_files_complete(sync_results)
            return Result.Ok(sync_results)
        sync_results = self.get_files_to_sync(local_files, s3_objects)
        self.events.get_sync_files_complete(sync_results)
        return Result.Ok(sync_results)

    def get_all_files_stored_locally_and_in_s3(self, file_type, data_set, year):
        local_files = self.get_all_objects_in_local_folder(file_type, data_set, year)
        s3_objects = self.get_all_objects_in_s3(file_type, data_set, year)
        result = self.check_both_locations_for_duplicates(s3_objects, local_files)
        if result.failure:
            return result
        return Result.Ok((s3_objects, local_files))

    def get_all_objects_in_local_folder(self, file_type, data_set, year):
        folderpath = self.scraped_data.get_local_folderpath(file_type, data_set, year)
        local_files = [
            self.get_local_file_data(file)
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

    def get_all_json_objects_in_s3(self, data_set, year):
        json_folder = self.s3_folderpath_dict[VigFile.PARSED_JSON][data_set].resolve(year=year)
        html_folder = self.s3_folderpath_dict[VigFile.SCRAPED_HTML][data_set].resolve(year=year)
        s3_objects = [
            self.get_s3_object_data(obj)
            for obj in self.cached_s3_objects
            if json_folder in obj.key
            and html_folder not in obj.key
            and URL_ID_REGEX[VigFile.PARSED_JSON][data_set].search(Path(obj.key).stem)
        ]
        return sorted(s3_objects, key=lambda x: x["name"]) if s3_objects else []

    def get_all_html_objects_in_s3(self, data_set, year):
        html_folder = self.s3_folderpath_dict[VigFile.SCRAPED_HTML][data_set].resolve(year=year)
        s3_objects = [
            self.get_s3_object_data(obj)
            for obj in self.cached_s3_objects
            if html_folder in obj.key
            and URL_ID_REGEX[VigFile.SCRAPED_HTML][data_set].search(Path(obj.key).stem)
        ]
        return sorted(s3_objects, key=lambda x: x["name"]) if s3_objects else []

    def get_all_combined_data_objects_in_s3(self, data_set, year):
        comb_folder = self.s3_folderpath_dict[VigFile.COMBINED_GAME_DATA][DataSet.ALL].resolve(
            year=year
        )
        s3_objects = [
            self.get_s3_object_data(obj)
            for obj in self.cached_s3_objects
            if comb_folder in obj.key
            and URL_ID_REGEX[VigFile.COMBINED_GAME_DATA][DataSet.ALL].search(Path(obj.key).stem)
        ]
        return sorted(s3_objects, key=lambda x: x["name"]) if s3_objects else []

    def get_all_patch_list_objects_in_s3(self, data_set, year):
        json_folder = self.s3_folderpath_dict[VigFile.PARSED_JSON][data_set].resolve(year=year)
        s3_objects = [
            self.get_s3_object_data(obj)
            for obj in self.cached_s3_objects
            if json_folder in obj.key
            and URL_ID_REGEX[VigFile.PATCH_LIST][data_set].search(Path(obj.key).stem)
        ]
        return sorted(s3_objects, key=lambda x: x["name"]) if s3_objects else []

    def get_local_file_data(self, file):
        return {
            "name": file.name,
            "path": file,
            "size": file.stat().st_size,
            "last_modified": dtaware_fromtimestamp(file.stat().st_mtime, use_tz=timezone.utc),
        }

    def get_s3_object_data(self, obj):
        return {
            "name": Path(obj.key).name,
            "path": Path(obj.key),
            "size": obj.size,
            "last_modified": obj.last_modified,
        }

    def check_both_locations_for_duplicates(self, s3_objects, local_files):
        s3_file_names = [f["name"] for f in s3_objects]
        local_file_names = [f["name"] for f in local_files]
        result_s3 = self.ensure_no_duplicates(s3_file_names)
        if result_s3.failure:
            result_s3 = Result.Fail(f"S3 bucket {result_s3.error}")
        result_local = self.ensure_no_duplicates(local_file_names)
        if result_local.failure:
            result_local = Result.Fail(f"Local folder {result_local.error}")
        return Result.Combine([result_s3, result_local])

    def ensure_no_duplicates(self, file_names):
        duplicates = Counter(file_names) - Counter(list(set(file_names)))
        if not duplicates:
            return Result.Ok()
        error = f"contains {len(duplicates)} duplicate file names:\n{', '.join(duplicates)}"
        return Result.Fail(error)

    def get_files_to_sync(self, src_files, dest_files):
        new_files = self.find_files_not_in_dest(src_files, dest_files)
        update_files = self.find_files_newer_in_src(src_files, dest_files)
        if not new_files and not update_files:
            return (False, [], [])
        return (True, new_files, update_files)

    def find_files_not_in_dest(self, src_files, dest_files):
        src_names = set(f["name"] for f in src_files if f["size"] > 0)
        dest_names = set(f["name"] for f in dest_files if f["size"] > 0)
        sync_files = list(src_names - dest_names)
        return [self.add_new_file(f) for f in src_files if f["name"] in sync_files]

    def add_new_file(self, file):
        file["operation"] = "ADD NEW FILE"
        return file

    def find_files_newer_in_src(self, src_files, dest_files):
        (src_sync, dest_sync) = self.find_files_in_common(src_files, dest_files)
        return [
            self.update_existing_file(src_sync[i])
            for i in range(len(src_sync))
            if self.src_file_is_newer(src_sync[i], dest_sync[i])
            and src_sync[i]["size"] != dest_sync[i]["size"]
        ]

    def update_existing_file(self, file):
        file["operation"] = "UPDATE EXISTING FILE"
        return file

    def src_file_is_newer(self, src, dest, min_diff_seconds=60):
        diff = src["last_modified"] - dest["last_modified"]
        return diff.total_seconds() > min_diff_seconds

    def find_files_in_common(self, src_files, dest_files):
        src_names = set(f["name"] for f in src_files if f["size"] > 0)
        dest_names = [f["name"] for f in dest_files if f["size"] > 0]
        sync_files = list(src_names.intersection(dest_names))
        src_sync = [f for f in src_files if f["name"] in sync_files]
        dest_sync = [f for f in dest_files if f["name"] in sync_files]
        src_sync.sort(key=lambda x: x["name"])
        dest_sync.sort(key=lambda x: x["name"])
        return (src_sync, dest_sync)

    def upload_files_to_s3(self, new_files, update_files, file_type, data_set, year):
        sync_files = self.get_sync_files(new_files, update_files)
        self.sync_files(SyncDirection.UP_TO_S3, sync_files, file_type, data_set, year)

    def download_files_to_local_folder(self, new_files, update_files, file_type, data_set, year):
        sync_files = self.get_sync_files(new_files, update_files)
        self.sync_files(SyncDirection.DOWN_TO_LOCAL, sync_files, file_type, data_set, year)

    def get_sync_files(self, new_files, update_files):
        if not new_files and not update_files:
            return []
        sync_files = []
        if new_files:
            sync_files = deepcopy(new_files)
        if update_files:
            sync_files.extend(deepcopy(update_files))
        return sync_files

    def sync_files(self, sync_direction, files, file_type, data_set, year):
        self.bucket_name = self.config.get_current_setting("S3_BUCKET", data_set)
        self.events.sync_files_start(files[0]["name"], 0, len(files))
        for num, file in enumerate(files, start=1):
            self.events.sync_files_progress(file["name"], num - 1, len(files))
            (filepath, s3_key) = self.get_local_and_s3_file_paths(file, file_type, data_set, year)
            self.send_file(sync_direction, filepath, s3_key)
            self.events.sync_files_progress(file["name"], num, len(files))
        self.events.sync_files_complete()

    def get_local_and_s3_file_paths(self, file, file_type, data_set, year):
        local_folder = self.scraped_data.get_local_folderpath(file_type, data_set, year)
        s3_folder = self.s3_folderpath_dict[file_type][data_set].resolve(year=year)
        local_path = Path(local_folder).joinpath(file["name"])
        s3_key = f"{s3_folder}{file['name']}"
        return (str(local_path), s3_key)

    def send_file(self, sync_direction, local_path, s3_key):
        if sync_direction == SyncDirection.UP_TO_S3:
            self.file_helper.client.upload_file(local_path, self.bucket_name, s3_key)
        if sync_direction == SyncDirection.DOWN_TO_LOCAL:
            self.file_helper.resource.Bucket(self.bucket_name).download_file(s3_key, local_path)
