"""Functions for reading and writing files."""
import json
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from vigorish.data.json_decoder import (
    decode_brooks_games_for_date,
    decode_brooks_pitch_logs_for_game,
    decode_brooks_pitchfx_log,
    decode_bbref_games_for_date,
    decode_bbref_boxscore,
)
from vigorish.enums import (
    DataSet,
    DocFormat,
    LocalFileTask,
    S3FileTask,
    HtmlStorageOption,
    JsonStorageOption,
)
from vigorish.util.dt_format_strings import DATE_ONLY, DATE_ONLY_TABLE_ID
from vigorish.util.result import Result


class FileHelper:
    def __init__(self, config):
        self.config = config
        self.bucket_name = self.config.all_settings.get("S3_BUCKET").current_setting(DataSet.ALL)
        self.client = boto3.client("s3")
        self.resource = boto3.resource("s3")
        self.bucket = self.resource.Bucket(self.bucket_name)

    @property
    def local_folderpath_dict(self):
        html_local_folder = self.config.all_settings.get("HTML_LOCAL_FOLDER_PATH")
        json_local_folder = self.config.all_settings.get("JSON_LOCAL_FOLDER_PATH")
        html_folderpath_dict = {
            data_set: html_local_folder.current_setting(data_set=data_set)
            for data_set in DataSet
            if data_set != DataSet.ALL
        }
        json_folderpath_dict = {
            data_set: json_local_folder.current_setting(data_set=data_set)
            for data_set in DataSet
            if data_set != DataSet.ALL
        }
        return {
            DocFormat.HTML: html_folderpath_dict,
            DocFormat.JSON: json_folderpath_dict,
        }

    @property
    def s3_folderpath_dict(self):
        html_s3_folder = self.config.all_settings.get("HTML_S3_FOLDER_PATH")
        json_s3_folder = self.config.all_settings.get("JSON_S3_FOLDER_PATH")
        html_folderpath_dict = {
            data_set: html_s3_folder.current_setting(data_set=data_set)
            for data_set in DataSet
            if data_set != DataSet.ALL
        }
        json_folderpath_dict = {
            data_set: json_s3_folder.current_setting(data_set=data_set)
            for data_set in DataSet
            if data_set != DataSet.ALL
        }
        return {
            DocFormat.HTML: html_folderpath_dict,
            DocFormat.JSON: json_folderpath_dict,
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
        return {
            DocFormat.HTML: html_filename_dict,
            DocFormat.JSON: json_filename_dict,
        }

    @property
    def json_decoder_dict(self):
        return {
            DataSet.BROOKS_GAMES_FOR_DATE: decode_brooks_games_for_date,
            DataSet.BROOKS_PITCH_LOGS: decode_brooks_pitch_logs_for_game,
            DataSet.BROOKS_PITCHFX: decode_brooks_pitchfx_log,
            DataSet.BBREF_GAMES_FOR_DATE: decode_bbref_games_for_date,
            DataSet.BBREF_BOXSCORES: decode_bbref_boxscore,
        }

    @property
    def file_storage_dict(self):
        html_storage = self.config.all_settings.get("HTML_STORAGE")
        json_storage = self.config.all_settings.get("JSON_STORAGE")
        html_storage_dict = {
            data_set: html_storage.current_setting(data_set=data_set)
            for data_set in DataSet
            if data_set != DataSet.ALL
        }
        json_storage_dict = {
            data_set: json_storage.current_setting(data_set=data_set)
            for data_set in DataSet
            if data_set != DataSet.ALL
        }
        return {
            DocFormat.HTML: html_storage_dict,
            DocFormat.JSON: json_storage_dict,
        }

    def perform_local_file_task(
        self,
        task,
        data_set,
        doc_format,
        game_date,
        scraped_data=None,
        bbref_game_id=None,
        bb_game_id=None,
        pitch_app_id=None,
        delete_file=False,
    ):
        filepath = self.get_local_filepath(
            data_set=data_set,
            doc_format=doc_format,
            game_date=game_date,
            bbref_game_id=bbref_game_id,
            bb_game_id=bb_game_id,
            pitch_app_id=pitch_app_id,
        )
        if task == LocalFileTask.READ_FILE:
            return self.read_local_file(Path(filepath))
        if task == LocalFileTask.WRITE_FILE:
            return self.write_to_file(scraped_data.as_json(), Path(filepath))
        if task == LocalFileTask.DELETE_FILE:
            return self.delete_file(Path(filepath))
        if task == LocalFileTask.DECODE_JSON:
            return self.decode_json(data_set, Path(filepath))

    def get_local_filepath(
        self,
        data_set,
        doc_format,
        game_date,
        bbref_game_id=None,
        bb_game_id=None,
        pitch_app_id=None,
    ):
        folderpath = self.get_local_folderpath(doc_format, data_set, game_date)
        filename = self.get_file_name(
            data_set, doc_format, game_date, bbref_game_id, bb_game_id, pitch_app_id,
        )
        return str(Path(folderpath).joinpath(filename))

    def get_local_folderpath(self, doc_format, data_set, game_date=None, year=None):
        if not game_date and not year:
            error = "You must provide either the game_date or year argument to construct a folderpath (FileHelper.get_s3_folderpath)"
            raise ValueError(error)
        year = year if year else game_date.year
        return self.local_folderpath_dict[doc_format][data_set].resolve(year=year)

    def get_file_name(
        self,
        data_set,
        doc_format,
        game_date,
        bbref_game_id=None,
        bb_game_id=None,
        pitch_app_id=None,
    ):
        if pitch_app_id:
            identifier = pitch_app_id
        elif bb_game_id:
            identifier = bb_game_id
        elif bbref_game_id:
            identifier = bbref_game_id
        elif game_date:
            identifier = game_date
        else:
            raise ValueError("Unable to construct file name. (FileHelper.get_file_name)")
        return self.filename_dict[doc_format][data_set](identifier)

    def perform_s3_task(
        self,
        task,
        data_set,
        doc_format,
        game_date,
        scraped_data=None,
        bbref_game_id=None,
        bb_game_id=None,
        pitch_app_id=None,
    ):
        s3_key = self.get_object_key(
            data_set=data_set,
            doc_format=doc_format,
            game_date=game_date,
            bbref_game_id=bbref_game_id,
            bb_game_id=bb_game_id,
            pitch_app_id=pitch_app_id,
        )
        filepath = self.get_local_filepath(
            data_set=data_set,
            doc_format=doc_format,
            game_date=game_date,
            bbref_game_id=bbref_game_id,
            bb_game_id=bb_game_id,
            pitch_app_id=pitch_app_id,
        )
        if task == S3FileTask.UPLOAD:
            return self.upload_to_s3(doc_format, data_set, scraped_data, s3_key, Path(filepath))
        if task == S3FileTask.DOWNLOAD:
            return self.download_from_s3(s3_key, Path(filepath))
        if task == S3FileTask.DELETE:
            return self.delete_from_s3(s3_key)

    def get_object_key(
        self,
        data_set,
        doc_format,
        game_date,
        bbref_game_id=None,
        bb_game_id=None,
        pitch_app_id=None,
    ):
        folderpath = self.get_s3_folderpath(doc_format, data_set, game_date)
        filename = self.get_file_name(
            data_set=data_set,
            doc_format=doc_format,
            game_date=game_date,
            bbref_game_id=bbref_game_id,
            bb_game_id=bb_game_id,
            pitch_app_id=pitch_app_id,
        )
        return f"{folderpath}{filename}"

    def get_s3_folderpath(self, doc_format, data_set, game_date=None, year=None):
        if not game_date and not year:
            error = "You must provide either the game_date or year argument to construct a folderpath (FileHelper.get_s3_folderpath)"
            raise ValueError(error)
        year = year if year else game_date.year
        return self.s3_folderpath_dict[doc_format][data_set].resolve(year=year)

    def get_file_name_html_brooks_games_for_date(self, game_date):
        return f"{game_date.strftime(DATE_ONLY_TABLE_ID)}.html"

    def get_file_name_json_brooks_games_for_date(self, game_date):
        return f"brooks_games_for_date_{game_date.strftime(DATE_ONLY)}.json"

    def get_file_name_html_brooks_pitch_log(self, pitch_app_id):
        return f"{pitch_app_id}.html"

    def get_file_name_json_brooks_pitch_log_for_game(self, bb_game_id):
        return f"{bb_game_id}.json"

    def get_file_name_html_brooks_pitchfx(self, pitch_app_id):
        return f"{pitch_app_id}.html"

    def get_file_name_json_brooks_pitchfx(self, pitch_app_id):
        return f"{pitch_app_id}.json"

    def get_file_name_html_bbref_games_for_date(self, game_date):
        return f"{game_date.strftime(DATE_ONLY_TABLE_ID)}.html"

    def get_file_name_json_bbref_games_for_date(self, game_date):
        return f"bbref_games_for_date_{game_date.strftime(DATE_ONLY)}.json"

    def get_file_name_html_bbref_boxscore(self, bbref_game_id):
        return f"{bbref_game_id}.html"

    def get_file_name_json_bbref_boxscore(self, bbref_game_id):
        return f"{bbref_game_id}.json"

    def read_local_file(self, filepath):
        return (
            Result.Ok(filepath)
            if filepath.exists()
            else Result.Fail(f"File not found: {filepath.resolve()}.")
        )

    def save_local_file(self, doc_format, data_set):
        storage_setting = self.file_storage_dict[doc_format][data_set]
        return "LOCAL_FOLDER" in storage_setting.name or "BOTH" in storage_setting.name

    def write_to_file(self, data, filepath):
        """Write object in json format to file."""
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

    def decode_json(self, data_set, filepath):
        delete_file = not self.save_local_file(DocFormat.JSON, data_set)
        try:
            contents = filepath.read_text()
            if delete_file:
                filepath.unlink()
            return self.json_decoder_dict[data_set](json.loads(contents))
        except Exception as e:
            error = f"Error: {repr(e)}"
            return Result.Fail(error)

    def upload_to_s3(self, doc_format, data_set, scraped_data, s3_key, filepath):
        delete_file = not self.save_local_file(doc_format, data_set)
        if doc_format == DocFormat.JSON:
            result = self.write_to_file(scraped_data.as_json(), filepath)
            if result.failure:
                return result
        try:
            self.client.upload_file(str(filepath), self.bucket_name, s3_key)
            if delete_file:
                filepath.unlink()
            return Result.Ok() if delete_file else Result.Ok(filepath)
        except Exception as e:
            return Result.Fail(f"Error: {repr(e)}")

    def download_from_s3(self, s3_key, filepath):
        try:
            self.resource.Bucket(self.bucket_name).download_file(s3_key, str(filepath))
            return Result.Ok(filepath)
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                error = f'The object "{s3_key}" does not exist.'
            else:
                error = repr(e)
            return Result.Fail(error)

    def delete_from_s3(self, s3_key):
        try:
            self.resource.Object(self.bucket_name, s3_key).delete()
            return Result.Ok()
        except ClientError as e:
            return Result.Fail(repr(e))

    def rename_s3_object(self, old_key, new_key):
        try:
            self.resource.Object(self.bucket_name, new_key).copy_from(
                CopySource=f"{self.bucket_name}/{old_key}"
            )
            self.resource.Object(self.bucket_name, old_key).delete()
            return Result.Ok()
        except ClientError as e:
            return Result.Fail(repr(e))