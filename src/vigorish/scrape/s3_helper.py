"""Functions for downloading from and uploading files to an Amazon S3 bucket."""
import errno
import json
import os
from datetime import datetime
from dateutil import parser
from enum import Enum, auto
from pathlib import Path
from string import Template

import boto3
from botocore.exceptions import ClientError

from vigorish.enums import DataSet, DocFormat, S3Task
from vigorish.models.status_date import DateScrapeStatus
from vigorish.models.status_pitch_appearance import PitchAppearanceScrapeStatus
from vigorish.util.dt_format_strings import DATE_ONLY, DATE_ONLY_2, DATE_ONLY_TABLE_ID
from vigorish.scrape.local_file_helper import LocalFileHelper
from vigorish.util.regex import PITCH_APP_REGEX
from vigorish.util.result import Result
from vigorish.util.string_helpers import (
    validate_brooks_game_id,
    validate_bbref_game_id,
    validate_pitch_app_id,
)


class S3Helper:
    """Perform CRUD operations on objects stored in an S3 bucket."""

    def __init__(self, config_file):
        self.config_file = config_file
        self.bucket_name = self.config_file.all_settings.get("S3_BUCKET").current_setting(
            DataSet.ALL
        )
        self.client = boto3.client("s3")
        self.resource = boto3.resource("s3")
        self.bucket = self.resource.Bucket(self.bucket_name)
        self.file_helper = LocalFileHelper(config_file)

    @property
    def s3_task_dict(self):
        return {
            S3Task.UPLOAD: self.upload_to_s3,
            S3Task.DOWNLOAD: self.download_from_s3,
            S3Task.DELETE: self.delete_from_s3,
        }

    @property
    def s3_folder_path_dict(self):
        html_s3_folder = self.config_file.all_settings.get("HTML_S3_FOLDER_PATH")
        json_s3_folder = self.config_file.all_settings.get("JSON_S3_FOLDER_PATH")
        html_s3_folder_dict = {
            data_set: html_s3_folder.current_setting(data_set=data_set)
            for data_set in DataSet
            if data_set != DataSet.ALL
        }
        json_s3_folder_dict = {
            data_set: json_s3_folder.current_setting(data_set=data_set)
            for data_set in DataSet
            if data_set != DataSet.ALL
        }
        return {
            DocFormat.HTML: html_s3_folder_dict,
            DocFormat.JSON: json_s3_folder_dict,
        }

    def get_brooks_games_for_date_from_s3(self, game_date, delete_file=True):
        """Retrieve BrooksGamesForDate object from json encoded file stored in S3."""
        result = self.download_json_brooks_games_for_date(game_date)
        if result.failure:
            return result
        return self.file_helper.decode_json_brooks_games_for_date(game_date, delete_file)

    def get_brooks_pitch_logs_for_game_from_s3(self, bb_game_id, delete_file=True):
        """Retrieve BrooksPitchLogsForGame object from json encoded file stored in S3."""
        result = self.download_json_brooks_pitch_logs_for_game(bb_game_id)
        if result.failure:
            return result
        return self.file_helper.decode_json_brooks_pitch_logs_for_game(bb_game_id, delete_file)

    def get_all_brooks_pitch_logs_for_date_from_s3(self, session, game_date, delete_file=True):
        """Retrieve a list of BrooksPitchLogsForGame objects for all games that occurred on a date."""
        brooks_game_ids = DateScrapeStatus.get_all_brooks_game_ids_for_date(session, game_date)
        pitch_logs = []
        for game_id in brooks_game_ids:
            result = self.get_brooks_pitch_logs_for_game_from_s3(game_id, delete_file)
            if result.failure:
                continue
            pitch_logs.append(result.value)
        return Result.Ok(pitch_logs)

    def get_brooks_pitchfx_log_from_s3(self, pitch_app_id, delete_file=True):
        """Retrieve BrooksPitchFxLog object from json encoded file stored in S3."""
        result = self.download_json_brooks_pitchfx_log(pitch_app_id)
        if result.failure:
            return result
        return self.file_helper.decode_json_brooks_pitchfx_log(pitch_app_id, delete_file)

    def get_all_pitchfx_logs_for_game_from_s3(self, session, bbref_game_id):
        pitch_app_ids = PitchAppearanceScrapeStatus.get_all_pitch_app_ids_for_game_with_pitchfx_data(
            session, bbref_game_id
        )
        fetch_tasks = [
            self.get_brooks_pitchfx_log_from_s3(pitch_app_id) for pitch_app_id in pitch_app_ids
        ]
        task_failed = any(result.failure for result in fetch_tasks)
        if task_failed:
            s3_errors = "\n".join(
                [f"Error: {result.error}" for result in fetch_tasks if result.failure]
            )
            error = (
                "The following errors occurred attempting to retrieve all pitchfx logs for game "
                f"{bbref_game_id}:\n{s3_errors}"
            )
            return Result.Fail(error)
        pitchfx_logs = [result.value for result in fetch_tasks]
        return Result.Ok(pitchfx_logs)

    def get_bbref_games_for_date_from_s3(self, game_date, delete_file=True):
        """Retrieve BBRefGamesForDate object from json encoded file stored in S3."""
        result = self.download_json_bbref_games_for_date(game_date)
        if result.failure:
            return result
        return self.file_helper.decode_json_bbref_games_for_date(game_date, delete_file)

    def get_bbref_boxscore_from_s3(self, bbref_game_id, delete_file=True):
        """Retrieve BBRefBoxscore object from json encoded file stored in S3."""
        result = self.download_json_bbref_boxscore(bbref_game_id)
        if result.failure:
            return result
        filepath = result.value
        return self.file_helper.decode_json_bbref_boxscore(bbref_game_id, delete_file)

    def get_all_brooks_dates_scraped_from_s3(self, year):
        json_folder = self.get_s3_folder_path(
            year=year, data_set=DataSet.BROOKS_GAMES_FOR_DATE, doc_format=DocFormat.JSON
        )
        html_folder = self.get_s3_key_for_folder(
            year=year, data_set=DataSet.BROOKS_GAMES_FOR_DATE, doc_format=DocFormat.HTML
        )
        scraped_dates = [
            parser.parse(Path(obj.key).stem[-10:])
            for obj in self.bucket.objects.all()
            if json_folder in obj.key and html_folder not in obj.key
        ]
        return Result.Ok(scraped_dates)

    def get_all_scraped_brooks_game_ids_from_s3(self, year):
        json_folder = self.get_s3_folder_path(
            year=year, data_set=DataSet.BROOKS_PITCH_LOGS, doc_format=DocFormat.JSON
        )
        html_folder = self.get_s3_key_for_folder(
            year=year, data_set=DataSet.BROOKS_PITCH_LOGS, doc_format=DocFormat.HTML
        )
        scraped_gameids = [
            Path(obj.key).stem
            for obj in self.bucket.objects.all()
            if json_folder in obj.key and html_folder not in obj.key
        ]
        return Result.Ok(scraped_gameids)

    def get_all_scraped_pitchfx_pitch_app_ids_from_s3(self, year):
        json_folder = self.get_s3_folder_path(
            year=year, data_set=DataSet.BROOKS_PITCHFX, doc_format=DocFormat.JSON
        )
        html_folder = self.get_s3_key_for_folder(
            year=year, data_set=DataSet.BROOKS_PITCHFX, doc_format=DocFormat.HTML
        )
        scraped_pitch_app_ids = [
            Path(obj.key).stem
            for obj in self.bucket.objects.all()
            if json_folder in obj.key and html_folder not in obj.key
        ]
        return Result.Ok(scraped_pitch_app_ids)

    def get_all_bbref_dates_scraped_from_s3(self, year):
        json_folder = self.get_s3_folder_path(
            year=year, data_set=DataSet.BBREF_GAMES_FOR_DATE, doc_format=DocFormat.JSON
        )
        html_folder = self.get_s3_key_for_folder(
            year=year, data_set=DataSet.BBREF_GAMES_FOR_DATE, doc_format=DocFormat.HTML
        )
        scraped_dates = [
            parser.parse(Path(obj.key).stem[-10:])
            for obj in self.bucket.objects.all()
            if json_folder in obj.key and html_folder not in obj.key
        ]
        return Result.Ok(scraped_dates)

    def get_all_scraped_bbref_game_ids_from_s3(self, year):
        json_folder = self.get_s3_folder_path(
            year=year, data_set=DataSet.BBREF_BOXSCORES, doc_format=DocFormat.JSON
        )
        html_folder = self.get_s3_key_for_folder(
            year=year, data_set=DataSet.BBREF_BOXSCORES, doc_format=DocFormat.HTML
        )
        scraped_game_ids = [
            Path(obj.key).stem
            for obj in self.bucket.objects.all()
            if json_folder in obj.key and html_folder not in obj.key
        ]
        return Result.Ok(scraped_game_ids)

    def upload_brooks_games_for_date(self, games_for_date):
        """Upload a file to S3 containing json encoded BrooksGamesForDate object."""
        result = write_json_brooks_games_for_date(games_for_date)
        if result.failure:
            return result
        return self.perform_task(
            task=S3Task.UPLOAD,
            doc_format=DocFormat.JSON,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            game_date=games_for_date.game_date,
        )

    def upload_brooks_pitch_logs_for_game(self, pitch_logs_for_game):
        """Upload a file to S3 containing json encoded BrooksPitchLogsForGame object."""
        result = write_json_brooks_pitch_logs_for_game(pitch_logs_for_game)
        if result.failure:
            return result
        return self.perform_task(
            task=S3Task.UPLOAD,
            doc_format=DocFormat.JSON,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            game_date=pitch_logs_for_game.game_date,
            bb_game_id=pitch_logs_for_game.bb_game_id,
        )

    def upload_brooks_pitchfx_log(self, pitchfx_log):
        """Upload a file to S3 containing json encoded BrooksPitchFxLog object."""
        result = write_json_brooks_pitchfx_log(pitchfx_log)
        if result.failure:
            return result
        return self.perform_task(
            task=S3Task.UPLOAD,
            doc_format=DocFormat.JSON,
            data_set=DataSet.BROOKS_PITCHFX,
            game_date=pitchfx_log.game_date,
            pitch_app_id=pitchfx_log.pitch_app_id,
        )

    def upload_bbref_games_for_date(self, games_for_date):
        """Upload a file to S3 containing json encoded BBRefGamesForDate object."""
        result = write_json_bbref_games_for_date(games_for_date)
        if result.failure:
            return result
        return self.perform_task(
            task=S3Task.UPLOAD,
            doc_format=DocFormat.JSON,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            game_date=games_for_date.game_date,
        )

    def upload_bbref_boxscore(self, boxscore):
        """Upload a file to S3 containing json encoded BBRefBoxscore object."""
        result = write_json_bbref_boxscore(boxscore)
        if result.failure:
            return result
        filepath = result.value
        game_date = boxscore.game_date
        return self.perform_task(
            task=S3Task.UPLOAD,
            doc_format=DocFormat.JSON,
            data_set=DataSet.BBREF_BOXSCORES,
            game_date=boxscore.game_date,
            bbref_game_id=boxscore.bbref_game_id,
        )

    def download_html_brooks_games_for_date(self, game_date):
        """Download raw HTML for brooks daily scoreboard page."""
        return self.perform_task(
            task=S3Task.DOWNLOAD,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            doc_format=DocFormat.HTML,
            game_date=game_date,
        )

    def download_json_brooks_games_for_date(self, game_date):
        """Download a file from S3 containing json encoded BrooksGamesForDate object."""
        return self.perform_task(
            task=S3Task.DOWNLOAD,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=game_date,
        )

    def download_html_brooks_pitch_log_page(self, pitch_app_id):
        """Download raw HTML for brooks pitch log page for a single pitching appearance."""
        result = validate_brooks_game_id(bb_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=S3Task.DOWNLOAD,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            doc_format=DocFormat.HTML,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def download_json_brooks_pitch_logs_for_game(self, bb_game_id):
        """Download a file from S3 containing json encoded BrooksPitchLogsForGame object."""
        result = validate_brooks_game_id(bb_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=S3Task.DOWNLOAD,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            bb_game_id=bb_game_id,
        )

    def download_html_brooks_pitchfx_log(self, pitch_app_id):
        """Download raw HTML for brooks pitchfx data for a single pitching appearance."""
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=S3Task.DOWNLOAD,
            doc_format=DocFormat.HTML,
            data_set=DataSet.BROOKS_PITCHFX,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def download_json_brooks_pitchfx_log(self, pitch_app_id):
        """Download a file from S3 containing json encoded BrooksPitchFxLog object."""
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=S3Task.DOWNLOAD,
            doc_format=DocFormat.JSON,
            data_set=DataSet.BROOKS_PITCHFX,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def download_html_bbref_games_for_date(self, game_date):
        """Download raw HTML for bbref daily scoreboard page."""
        return self.perform_task(
            task=S3Task.DOWNLOAD,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            doc_format=DocFormat.HTML,
            game_date=game_date,
        )

    def download_json_bbref_games_for_date(self, game_date):
        """Download a file from S3 containing json encoded BBRefGamesForDate object."""
        return self.perform_task(
            task=S3Task.DOWNLOAD,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=game_date,
        )

    def download_html_bbref_boxscore(self, bbref_game_id):
        """Download raw HTML for bbref daily scoreboard page."""
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=S3Task.DOWNLOAD,
            doc_format=DocFormat.HTML,
            data_set=DataSet.BBREF_BOXSCORES,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def download_json_bbref_boxscore(self, bbref_game_id):
        """Download a file from S3 containing json encoded BBRefBoxscore object."""
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=S3Task.DOWNLOAD,
            doc_format=DocFormat.JSON,
            data_set=DataSet.BBREF_BOXSCORES,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def delete_brooks_games_for_date_from_s3(self, game_date):
        return self.perform_task(
            task=S3Task.DELETE,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=game_date,
        )

    def delete_brooks_pitch_logs_for_game_from_s3(self, bb_game_id):
        """Delete brooks pitch logs for game from s3."""
        result = validate_brooks_game_id(bb_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=S3Task.DELETE,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            bb_game_id=bb_game_id,
        )

    def delete_brooks_pitchfx_log_from_s3(self, pitch_app_id):
        """Delete a pitchfx log from s3 based on the pitch_app_id value."""
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=S3Task.DELETE,
            doc_format=DocFormat.JSON,
            data_set=DataSet.BROOKS_PITCHFX,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def delete_bbref_games_for_date_from_s3(self, game_date):
        """Delete bbref_games_for_date from s3."""
        return self.perform_task(
            task=S3Task.DELETE,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=game_date,
        )

    def delete_bbref_boxscore_from_s3(self, bbref_game_id):
        """Delete a bbref boxscore from s3."""
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=S3Task.DELETE,
            doc_format=DocFormat.JSON,
            data_set=DataSet.BBREF_BOXSCORES,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def rename_brooks_pitchfx_log(self, old_pitch_app_id, new_pitch_app_id, year):
        result = validate_pitch_app_id(old_pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        old_key = self.get_object_key(
            doc_format=DocFormat.JSON,
            data_set=DataSet.BROOKS_PITCHFX,
            game_date=game_dict["game_date"],
            pitch_app_id=old_pitch_app_id,
        )

        result = validate_pitch_app_id(new_pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        new_key = self.get_object_key(
            doc_format=DocFormat.JSON,
            data_set=DataSet.BROOKS_PITCHFX,
            game_date=game_dict["game_date"],
            pitch_app_id=new_pitch_app_id,
        )
        return rename_s3_object(old_key, new_key)

    def perform_task(
        self,
        task,
        data_set,
        doc_format,
        game_date,
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
        local_file_path = self.file_helper.get_local_file_path(
            data_set=data_set,
            doc_format=doc_format,
            game_date=game_date,
            bbref_game_id=bbref_game_id,
            bb_game_id=bb_game_id,
            pitch_app_id=pitch_app_id,
        )
        return self.s3_task_dict[task](s3_key, local_file_path)

    def get_object_key(
        self,
        data_set,
        doc_format,
        game_date,
        bbref_game_id=None,
        bb_game_id=None,
        pitch_app_id=None,
    ):
        folderpath = self.get_s3_folder_path(
            year=game_date.year, data_set=data_set, doc_format=doc_format
        )
        filename = self.file_helper.get_file_name(
            data_set=data_set,
            doc_format=doc_format,
            game_date=game_date,
            bbref_game_id=bbref_game_id,
            bb_game_id=bb_game_id,
            pitch_app_id=pitch_app_id,
        )
        return f"{folderpath}/{filename}"

    def get_s3_folder_path(self, year, data_set, doc_format):
        return self.s3_folder_path_dict[doc_format][data_set].resolve(year=year)

    def upload_to_s3(self, s3_key, filepath):
        """Upload the specified file to S3. """
        try:
            self.client.upload_file(filepath, self.bucket_name, s3_key)
            Path(filepath).unlink()
            return Result.Ok()
        except Exception as e:
            return Result.Fail(f"Error: {repr(e)}")

    def download_from_s3(self, s3_key, filepath):
        try:
            self.resource.Bucket(self.bucket_name).download_file(s3_key, filepath)
            return Result.Ok(Path(filepath))
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                error = f'The object "{s3_key}" does not exist.'
            else:
                error = repr(e)
            return Result.Fail(error)

    def delete_from_s3(self, s3_key, filepath):
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
