"""Functions for reading and writing files."""
from pathlib import Path

from vigorish.enums import DataSet, DocFormat, LocalFileTask, S3FileTask
from vigorish.data.file_helper import FileHelper
from vigorish.util.dt_format_strings import DATE_ONLY
from vigorish.util.result import Result
from vigorish.util.string_helpers import (
    validate_bbref_game_id,
    validate_brooks_game_id,
    validate_pitch_app_id,
)


class JsonStorage:
    """Perform CRUD operations on JSON files stored locally and/or in S3."""

    def __init__(self, config, file_helper):
        self.config = config
        self.file_helper = file_helper

    def write_json_brooks_games_for_date_local_file(self, games_for_date):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=games_for_date.game_date,
            scraped_data=games_for_date,
        )

    def write_json_brooks_games_for_date_s3(self, games_for_date):
        return self.file_helper.perform_s3_task(
            task=S3FileTask.UPLOAD,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=games_for_date.game_date,
            scraped_data=games_for_date,
        )

    def write_json_brooks_pitch_logs_for_game_local_file(self, pitch_logs_for_game):
        result = validate_brooks_game_id(pitch_logs_for_game.bb_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            scraped_data=pitch_logs_for_game,
            bb_game_id=pitch_logs_for_game.bb_game_id,
        )

    def write_json_brooks_pitch_logs_for_game_s3(self, pitch_logs_for_game):
        result = validate_brooks_game_id(pitch_logs_for_game.bb_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.UPLOAD,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            scraped_data=pitch_logs_for_game,
            bb_game_id=pitch_logs_for_game.bb_game_id,
        )

    def write_json_brooks_pitchfx_log_local_file(self, pitchfx_log):
        result = validate_pitch_app_id(pitchfx_log.pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BROOKS_PITCHFX,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            scraped_data=pitchfx_log,
            pitch_app_id=pitchfx_log.pitch_app_id,
        )

    def write_json_brooks_pitchfx_log_s3(self, pitchfx_log):
        result = validate_pitch_app_id(pitchfx_log.pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.UPLOAD,
            data_set=DataSet.BROOKS_PITCHFX,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            scraped_data=pitchfx_log,
            pitch_app_id=pitchfx_log.pitch_app_id,
        )

    def write_json_bbref_games_for_date_local_file(self, games_for_date):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=games_for_date.game_date,
            scraped_data=games_for_date,
        )

    def write_json_bbref_games_for_date_s3(self, games_for_date):
        return self.file_helper.perform_s3_task(
            task=S3FileTask.UPLOAD,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=games_for_date.game_date,
            scraped_data=games_for_date,
        )

    def write_json_bbref_boxscore_local_file(self, boxscore):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BBREF_BOXSCORES,
            doc_format=DocFormat.JSON,
            game_date=boxscore.game_date,
            scraped_data=boxscore,
            bbref_game_id=boxscore.bbref_game_id,
        )

    def write_json_bbref_boxscore_s3(self, boxscore):
        return self.file_helper.perform_s3_task(
            task=S3FileTask.UPLOAD,
            data_set=DataSet.BBREF_BOXSCORES,
            doc_format=DocFormat.JSON,
            game_date=boxscore.game_date,
            scraped_data=boxscore,
            bbref_game_id=boxscore.bbref_game_id,
        )

    def get_json_brooks_games_for_date_local_file(self, game_date):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=game_date,
        )

    def get_json_brooks_games_for_date_s3(self, game_date):
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DOWNLOAD,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=game_date,
        )

    def get_json_brooks_pitch_logs_for_game_local_file(self, bb_game_id):
        result = validate_brooks_game_id(bb_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            bb_game_id=bb_game_id,
        )

    def get_json_brooks_pitch_logs_for_game_s3(self, bb_game_id):
        result = validate_brooks_game_id(bb_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DOWNLOAD,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            bb_game_id=bb_game_id,
        )

    def get_json_brooks_pitchfx_local_file(self, pitch_app_id):
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BROOKS_PITCHFX,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def get_json_brooks_pitchfx_s3(self, pitch_app_id):
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DOWNLOAD,
            data_set=DataSet.BROOKS_PITCHFX,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def get_json_bbref_games_for_date_local_file(self, game_date):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=game_date,
        )

    def get_json_bbref_games_for_date_s3(self, game_date):
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DOWNLOAD,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=game_date,
        )

    def get_json_bbref_boxscore_local_file(self, bbref_game_id):
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BBREF_BOXSCORES,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def get_json_bbref_boxscore_s3(self, bbref_game_id):
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DOWNLOAD,
            data_set=DataSet.BBREF_BOXSCORES,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def decode_json_brooks_games_for_date_local_file(self, game_date):
        result = self.get_json_brooks_games_for_date_local_file(game_date)
        if result.failure:
            return result
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DECODE_JSON,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=game_date,
            delete_file=False,
        )

    def decode_json_brooks_games_for_date_s3(self, game_date):
        result = self.get_json_brooks_games_for_date_s3(game_date)
        if result.failure:
            return result
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DECODE_JSON,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=game_date,
            delete_file=True,
        )

    def decode_json_brooks_pitch_logs_for_game_local_file(self, bb_game_id):
        result = self.get_json_brooks_pitch_logs_for_game_local_file(bb_game_id)
        if result.failure:
            return result
        result = validate_brooks_game_id(bb_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DECODE_JSON,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            bb_game_id=bb_game_id,
            delete_file=False,
        )

    def decode_json_brooks_pitch_logs_for_game_s3(self, bb_game_id):
        result = self.get_json_brooks_pitch_logs_for_game_s3(bb_game_id)
        if result.failure:
            return result
        result = validate_brooks_game_id(bb_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DECODE_JSON,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            bb_game_id=bb_game_id,
            delete_file=True,
        )

    def decode_json_brooks_pitchfx_log_local_file(self, pitch_app_id):
        result = self.get_json_brooks_pitchfx_local_file(pitch_app_id)
        if result.failure:
            return result
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DECODE_JSON,
            data_set=DataSet.BROOKS_PITCHFX,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
            delete_file=False,
        )

    def decode_json_brooks_pitchfx_log_s3(self, pitch_app_id):
        result = self.get_json_brooks_pitchfx_s3(pitch_app_id)
        if result.failure:
            return result
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DECODE_JSON,
            data_set=DataSet.BROOKS_PITCHFX,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
            delete_file=True,
        )

    def decode_json_bbref_games_for_date_local_file(self, game_date):
        result = self.get_json_bbref_games_for_date_local_file(game_date)
        if result.failure:
            return result
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DECODE_JSON,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=game_date,
            delete_file=False,
        )

    def decode_json_bbref_games_for_date_s3(self, game_date):
        result = self.get_json_bbref_games_for_date_s3(game_date)
        if result.failure:
            return result
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DECODE_JSON,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=game_date,
            delete_file=True,
        )

    def decode_json_bbref_boxscore_local_file(self, bbref_game_id):
        result = self.get_json_bbref_boxscore_local_file(bbref_game_id)
        if result.failure:
            return result
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DECODE_JSON,
            data_set=DataSet.BBREF_BOXSCORES,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
            delete_file=False,
        )

    def decode_json_bbref_boxscore_s3(self, bbref_game_id):
        result = self.get_json_bbref_boxscore_s3(bbref_game_id)
        if result.failure:
            return result
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DECODE_JSON,
            data_set=DataSet.BBREF_BOXSCORES,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
            delete_file=True,
        )

    def delete_brooks_games_for_date_local_file(self, game_date):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DELETE_FILE,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=game_date,
        )

    def delete_brooks_games_for_date_s3(self, game_date):
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DELETE,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=game_date,
        )

    def delete_brooks_pitch_logs_for_game_local_file(self, bb_game_id):
        result = validate_brooks_game_id(bb_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DELETE_FILE,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            bb_game_id=bb_game_id,
        )

    def delete_brooks_pitch_logs_for_game_s3(self, bb_game_id):
        result = validate_brooks_game_id(bb_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DELETE,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            bb_game_id=bb_game_id,
        )

    def delete_brooks_pitchfx_log_local_file(self, pitch_app_id):
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DELETE_FILE,
            data_set=DataSet.BROOKS_PITCHFX,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def delete_brooks_pitchfx_log_s3(self, pitch_app_id):
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DELETE,
            data_set=DataSet.BROOKS_PITCHFX,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def delete_bbref_games_for_date_local_file(self, game_date):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DELETE_FILE,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=game_date,
        )

    def delete_bbref_games_for_date_s3(self, game_date):
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DELETE,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=game_date,
        )

    def delete_bbref_boxscore_local_file(self, bbref_game_id):
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DELETE_FILE,
            data_set=DataSet.BBREF_BOXSCORES,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def delete_bbref_boxscore_s3(self, bbref_game_id):
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DELETE,
            data_set=DataSet.BBREF_BOXSCORES,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def rename_brooks_pitchfx_log(self, old_pitch_app_id, new_pitch_app_id, year):
        result = validate_pitch_app_id(old_pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        old_key = self.file_helper.get_object_key(
            doc_format=DocFormat.JSON,
            data_set=DataSet.BROOKS_PITCHFX,
            game_date=game_dict["game_date"],
            pitch_app_id=old_pitch_app_id,
        )
        result = validate_pitch_app_id(new_pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        new_key = self.file_helper.get_object_key(
            doc_format=DocFormat.JSON,
            data_set=DataSet.BROOKS_PITCHFX,
            game_date=game_dict["game_date"],
            pitch_app_id=new_pitch_app_id,
        )
        return self.file_helper.rename_s3_object(old_key, new_key)
