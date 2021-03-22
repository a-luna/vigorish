"""Functions for reading and writing files."""
import json
import os
from pathlib import Path

import boto3
import botocore

from vigorish.data.json_decoder import (
    decode_bbref_boxscore,
    decode_bbref_boxscore_patch_list,
    decode_bbref_games_for_date,
    decode_bbref_games_for_date_patch_list,
    decode_brooks_games_for_date,
    decode_brooks_games_for_date_patch_list,
    decode_brooks_pitch_logs_for_game,
    decode_brooks_pitchfx_log,
    decode_brooks_pitchfx_patch_list,
)
from vigorish.enums import DataSet, LocalFileTask, S3FileTask, VigFile
from vigorish.util.dt_format_strings import DATE_ONLY, DATE_ONLY_TABLE_ID
from vigorish.util.exceptions import S3BucketException
from vigorish.util.numeric_helpers import ONE_KB
from vigorish.util.result import Result


class FileHelper:
    def __init__(self, config):
        self.config = config
        self.bucket_name = self.config.get_current_setting("S3_BUCKET", DataSet.ALL)
        self.s3_resource = None
        try:  # pragma: no cover
            if os.environ.get("ENV") != "TEST":
                self.s3_resource = boto3.resource("s3")
        except (boto3.exceptions.ResourceNotExistsError, ValueError):  # pragma: no cover
            self.s3_resource = None

    @property
    def local_folderpath_dict(self):
        html_local_folder = self.config.all_settings.get("HTML_LOCAL_FOLDER_PATH")
        json_local_folder = self.config.all_settings.get("JSON_LOCAL_FOLDER_PATH")
        combined_local_folder = self.config.all_settings.get("COMBINED_DATA_LOCAL_FOLDER_PATH")
        return {
            VigFile.SCRAPED_HTML: {ds: html_local_folder.current_setting(data_set=ds) for ds in DataSet},
            VigFile.PARSED_JSON: {ds: json_local_folder.current_setting(data_set=ds) for ds in DataSet},
            VigFile.PATCH_LIST: {ds: json_local_folder.current_setting(data_set=ds) for ds in DataSet},
            VigFile.COMBINED_GAME_DATA: {DataSet.ALL: combined_local_folder.current_setting(data_set=DataSet.ALL)},
        }

    @property
    def s3_folderpath_dict(self):  # pragma: no cover
        html_s3_folder = self.config.all_settings.get("HTML_S3_FOLDER_PATH")
        json_s3_folder = self.config.all_settings.get("JSON_S3_FOLDER_PATH")
        combined_s3_folder = self.config.all_settings.get("COMBINED_DATA_S3_FOLDER_PATH")
        return {
            VigFile.SCRAPED_HTML: {ds: html_s3_folder.current_setting(data_set=ds) for ds in DataSet},
            VigFile.PARSED_JSON: {ds: json_s3_folder.current_setting(data_set=ds) for ds in DataSet},
            VigFile.PATCH_LIST: {ds: json_s3_folder.current_setting(data_set=ds) for ds in DataSet},
            VigFile.COMBINED_GAME_DATA: {DataSet.ALL: combined_s3_folder.current_setting(data_set=DataSet.ALL)},
        }

    @property
    def filename_dict(self):
        html_filename_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.get_file_name_html_brooks_games_for_date,
            DataSet.BROOKS_PITCH_LOGS: self.get_file_name_html_brooks_pitch_log,
            DataSet.BROOKS_PITCHFX: self.get_file_name_html_brooks_pitchfx,
            DataSet.BBREF_GAMES_FOR_DATE: self.get_file_name_html_bbref_games_for_date,
            DataSet.BBREF_BOXSCORES: self.get_file_name_html_bbref_boxscore,
        }
        json_filename_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.get_file_name_json_brooks_games_for_date,
            DataSet.BROOKS_PITCH_LOGS: self.get_file_name_json_brooks_pitch_log_for_game,
            DataSet.BROOKS_PITCHFX: self.get_file_name_json_brooks_pitchfx,
            DataSet.BBREF_GAMES_FOR_DATE: self.get_file_name_json_bbref_games_for_date,
            DataSet.BBREF_BOXSCORES: self.get_file_name_json_bbref_boxscore,
        }
        patch_list_filename_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.get_file_name_brooks_games_for_date_patch_list,
            DataSet.BROOKS_PITCHFX: self.get_file_name_brooks_pitchfx_patch_list,
            DataSet.BBREF_GAMES_FOR_DATE: self.get_file_name_bbref_games_for_date_patch_list,
            DataSet.BBREF_BOXSCORES: self.get_file_name_bbref_boxscore_patch_list,
        }
        return {
            VigFile.SCRAPED_HTML: html_filename_dict,
            VigFile.PARSED_JSON: json_filename_dict,
            VigFile.PATCH_LIST: patch_list_filename_dict,
            VigFile.COMBINED_GAME_DATA: {DataSet.ALL: self.get_file_name_combined_game_data},
        }

    @property
    def json_decoder_dict(self):
        scraped_data_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: decode_brooks_games_for_date,
            DataSet.BROOKS_PITCH_LOGS: decode_brooks_pitch_logs_for_game,
            DataSet.BROOKS_PITCHFX: decode_brooks_pitchfx_log,
            DataSet.BBREF_GAMES_FOR_DATE: decode_bbref_games_for_date,
            DataSet.BBREF_BOXSCORES: decode_bbref_boxscore,
        }
        patch_file_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: decode_brooks_games_for_date_patch_list,
            DataSet.BROOKS_PITCHFX: decode_brooks_pitchfx_patch_list,
            DataSet.BBREF_GAMES_FOR_DATE: decode_bbref_games_for_date_patch_list,
            DataSet.BBREF_BOXSCORES: decode_bbref_boxscore_patch_list,
        }
        return {
            VigFile.PARSED_JSON: scraped_data_dict,
            VigFile.PATCH_LIST: patch_file_dict,
        }

    @property
    def file_storage_dict(self):
        html_storage = self.config.all_settings.get("HTML_STORAGE")
        json_storage = self.config.all_settings.get("JSON_STORAGE")
        combined_storage = self.config.all_settings.get("COMBINED_DATA_STORAGE")
        return {
            VigFile.SCRAPED_HTML: {ds: html_storage.current_setting(data_set=ds) for ds in DataSet},
            VigFile.PARSED_JSON: {ds: json_storage.current_setting(data_set=ds) for ds in DataSet},
            VigFile.PATCH_LIST: {ds: json_storage.current_setting(data_set=ds) for ds in DataSet},
            VigFile.COMBINED_GAME_DATA: {DataSet.ALL: combined_storage.current_setting(data_set=DataSet.ALL)},
        }

    def create_all_folderpaths(self, year):
        try:
            for folderpath_dict in self.local_folderpath_dict.values():
                for folderpath in folderpath_dict.values():
                    Path(folderpath.resolve(year=year)).mkdir(parents=True, exist_ok=True)
            return Result.Ok()
        except Exception as e:
            return Result.Fail(f"Error: {repr(e)}")

    def check_s3_bucket(self):  # pragma: no cover
        if not self.s3_resource:
            raise S3BucketException()
        try:
            s3_bucket = self.s3_resource.Bucket(self.bucket_name) if self.bucket_name else None
            return Result.Ok(s3_bucket)
        except botocore.exceptions.ClientError as ex:
            raise S3BucketException(f'{repr(ex)} (Error Code: {ex.response["Error"]["Code"]})')

    def check_file_stored_local(self, file_type, data_set):
        storage_setting = self.file_storage_dict[file_type][data_set]
        return "LOCAL_FOLDER" in storage_setting.name or "BOTH" in storage_setting.name

    def check_file_stored_s3(self, file_type, data_set):  # pragma: no cover
        storage_setting = self.file_storage_dict[file_type][data_set]
        return "S3_BUCKET" in storage_setting.name or "BOTH" in storage_setting.name

    def get_s3_bucket(self):  # pragma: no cover
        result = self.check_s3_bucket()
        return None if result.failure else result.value

    def get_all_object_keys_in_s3_bucket(self):  # pragma: no cover
        s3_bucket = self.get_s3_bucket()
        return list(s3_bucket.objects.all()) if s3_bucket else []

    def perform_local_file_task(
        self,
        task,
        data_set,
        file_type,
        game_date,
        scraped_data=None,
        bbref_game_id=None,
        bb_game_id=None,
        pitch_app_id=None,
        delete_file=False,
    ):
        filepath = self.get_local_filepath(
            data_set=data_set,
            file_type=file_type,
            game_date=game_date,
            bbref_game_id=bbref_game_id,
            bb_game_id=bb_game_id,
            pitch_app_id=pitch_app_id,
        )
        if task == LocalFileTask.READ_FILE:
            return self.read_local_file(Path(filepath))
        if task == LocalFileTask.WRITE_FILE:
            return self.write_to_file(file_type, scraped_data, Path(filepath))
        if task == LocalFileTask.DELETE_FILE:
            return self.delete_file(Path(filepath))
        if task == LocalFileTask.DECODE_JSON:
            return self.decode_json(file_type, data_set, Path(filepath))

    def get_local_filepath(
        self,
        data_set,
        file_type,
        game_date,
        bbref_game_id=None,
        bb_game_id=None,
        pitch_app_id=None,
    ):
        folderpath = self.get_local_folderpath(file_type, data_set, game_date)
        filename = self.get_file_name(
            data_set,
            file_type,
            game_date,
            bbref_game_id,
            bb_game_id,
            pitch_app_id,
        )
        return str(Path(folderpath).joinpath(filename))

    def get_local_folderpath(self, file_type, data_set, game_date=None, year=None):
        if not game_date and not year:
            error = "You must provide either the game_date or year argument to construct a folderpath"
            raise ValueError(error)
        year = year or game_date.year
        return self.local_folderpath_dict[file_type][data_set].resolve(year=year)

    def get_file_name(
        self,
        data_set,
        file_type,
        game_date,
        bbref_game_id=None,
        bb_game_id=None,
        pitch_app_id=None,
    ):
        if pitch_app_id:
            url_id = pitch_app_id
        elif bb_game_id:
            url_id = bb_game_id
        elif bbref_game_id:
            url_id = bbref_game_id
        elif game_date:
            url_id = game_date
        else:
            raise ValueError("Unable to construct file name.")
        return self.filename_dict[file_type][data_set](url_id) if data_set in self.filename_dict[file_type] else None

    def perform_s3_task(
        self,
        task,
        data_set,
        file_type,
        game_date,
        scraped_data=None,
        bbref_game_id=None,
        bb_game_id=None,
        pitch_app_id=None,
    ):  # pragma: no cover
        s3_key = self.get_s3_object_key(
            data_set=data_set,
            file_type=file_type,
            game_date=game_date,
            bbref_game_id=bbref_game_id,
            bb_game_id=bb_game_id,
            pitch_app_id=pitch_app_id,
        )
        filepath = self.get_local_filepath(
            data_set=data_set,
            file_type=file_type,
            game_date=game_date,
            bbref_game_id=bbref_game_id,
            bb_game_id=bb_game_id,
            pitch_app_id=pitch_app_id,
        )
        if task == S3FileTask.UPLOAD:
            return self.upload_to_s3(file_type, data_set, scraped_data, s3_key, Path(filepath))
        if task == S3FileTask.DOWNLOAD:
            return self.download_from_s3(file_type, s3_key, Path(filepath))
        if task == S3FileTask.DELETE:
            return self.delete_from_s3(s3_key)

    def get_s3_object_key(
        self,
        data_set,
        file_type,
        game_date,
        bbref_game_id=None,
        bb_game_id=None,
        pitch_app_id=None,
    ):  # pragma: no cover
        folderpath = self.get_s3_folderpath(file_type, data_set, game_date)
        filename = self.get_file_name(
            data_set=data_set,
            file_type=file_type,
            game_date=game_date,
            bbref_game_id=bbref_game_id,
            bb_game_id=bb_game_id,
            pitch_app_id=pitch_app_id,
        )
        return f"{folderpath}/{filename}"

    def get_s3_folderpath(self, file_type, data_set, game_date=None, year=None):  # pragma: no cover
        if not game_date and not year:
            error = "You must provide either the game_date or year argument to construct a folderpath"
            raise ValueError(error)
        year = year or game_date.year
        return self.s3_folderpath_dict[file_type][data_set].resolve(year=year)

    def get_file_name_html_brooks_games_for_date(self, game_date):
        return f"{game_date.strftime(DATE_ONLY_TABLE_ID)}.html"

    def get_file_name_json_brooks_games_for_date(self, game_date):
        return f"brooks_games_for_date_{game_date.strftime(DATE_ONLY)}.json"

    def get_file_name_brooks_games_for_date_patch_list(self, game_date):
        return f"brooks_games_for_date_{game_date.strftime(DATE_ONLY)}_PATCH_LIST.json"

    def get_file_name_html_brooks_pitch_log(self, pitch_app_id):
        return f"{pitch_app_id}.html"

    def get_file_name_json_brooks_pitch_log_for_game(self, bb_game_id):
        return f"{bb_game_id}.json"

    def get_file_name_html_brooks_pitchfx(self, pitch_app_id):
        return f"{pitch_app_id}.html"

    def get_file_name_json_brooks_pitchfx(self, pitch_app_id):
        return f"{pitch_app_id}.json"

    def get_file_name_brooks_pitchfx_patch_list(self, bbref_game_id):
        return f"{bbref_game_id}_PATCH_LIST.json"

    def get_file_name_html_bbref_games_for_date(self, game_date):
        return f"{game_date.strftime(DATE_ONLY_TABLE_ID)}.html"

    def get_file_name_json_bbref_games_for_date(self, game_date):
        return f"bbref_games_for_date_{game_date.strftime(DATE_ONLY)}.json"

    def get_file_name_bbref_games_for_date_patch_list(self, game_date):
        return f"bbref_games_for_date_{game_date.strftime(DATE_ONLY)}_PATCH_LIST.json"

    def get_file_name_html_bbref_boxscore(self, bbref_game_id):
        return f"{bbref_game_id}.html"

    def get_file_name_json_bbref_boxscore(self, bbref_game_id):
        return f"{bbref_game_id}.json"

    def get_file_name_bbref_boxscore_patch_list(self, bbref_game_id):
        return f"{bbref_game_id}_PATCH_LIST.json"

    def get_file_name_combined_game_data(self, bbref_game_id):
        return f"{bbref_game_id}_COMBINED_DATA.json"

    def read_local_file(self, filepath):
        return Result.Ok(filepath) if filepath.exists() else Result.Fail(f"File not found: {filepath.resolve()}.")

    def write_to_file(self, file_type, data, filepath):
        """Write object in json format to file."""
        if file_type in [VigFile.PARSED_JSON, VigFile.PATCH_LIST]:
            data = data.as_json()
        if file_type == VigFile.COMBINED_GAME_DATA:
            data = json.dumps(data, indent=2, sort_keys=False)
        try:
            filepath.write_text(data)
            return Result.Ok(filepath)
        except Exception as e:
            error = f"Error: {repr(e)}"
            return Result.Fail(error)

    def delete_file(self, filepath):
        if filepath.exists() and filepath.is_file():
            filepath.unlink()
        return Result.Ok()

    def decode_json(self, file_type, data_set, filepath):
        delete_file = not self.check_file_stored_local(VigFile.PARSED_JSON, data_set)
        try:
            contents = filepath.read_text()
            if delete_file and os.environ.get("ENV") != "TEST":
                filepath.unlink()
            return self.json_decoder_dict[file_type][data_set](json.loads(contents))
        except Exception as e:
            error = f"Error: {repr(e)}"
            return Result.Fail(error)

    def upload_to_s3(self, file_type, data_set, scraped_data, s3_key, filepath):  # pragma: no cover
        delete_file = not self.check_file_stored_local(file_type, data_set)
        if file_type == VigFile.PARSED_JSON:
            result = self.write_to_file(file_type, scraped_data, filepath)
            if result.failure:
                return result
        try:
            self.get_s3_bucket().upload_file(str(filepath), s3_key)
            if delete_file:
                filepath.unlink()
            return Result.Ok() if delete_file else Result.Ok(filepath)
        except botocore.exceptions.ClientError as ex:
            error_code = ex.response["Error"]["Code"]
            return Result.Fail(f"{repr(ex)} (Error Code: {error_code})")

    def download_from_s3(self, file_type, s3_key, filepath):  # pragma: no cover
        try:
            self.get_s3_bucket().download_file(s3_key, str(filepath))
            if file_type == VigFile.SCRAPED_HTML and filepath.stat().st_size < ONE_KB:
                self.s3_resource.Object(self.bucket_name, s3_key).delete()
                filepath.unlink()
                error = f"Size of file downloaded from S3 is less than 1KB ({s3_key})"
                return Result.Fail(error)
            return Result.Ok(filepath)
        except botocore.exceptions.ClientError as ex:
            error_code = ex.response["Error"]["Code"]
            if error_code == "404":
                error = f'The object "{s3_key}" does not exist.'
            else:
                error = f"{repr(ex)} (Error Code: {error_code})"
            return Result.Fail(error)

    def delete_from_s3(self, s3_key):  # pragma: no cover
        try:
            self.s3_resource.Object(self.bucket_name, s3_key).delete()
            return Result.Ok()
        except botocore.exceptions.ClientError as ex:
            error_code = ex.response["Error"]["Code"]
            return Result.Fail(f"{repr(ex)} (Error Code: {error_code})")

    def rename_s3_object(self, old_key, new_key):  # pragma: no cover
        try:
            self.s3_resource.Object(self.bucket_name, new_key).copy_from(CopySource=f"{self.bucket_name}/{old_key}")
            self.s3_resource.Object(self.bucket_name, old_key).delete()
            return Result.Ok()
        except botocore.exceptions.ClientError as ex:
            error_code = ex.response["Error"]["Code"]
            return Result.Fail(f"{repr(ex)} (Error Code: {error_code})")
