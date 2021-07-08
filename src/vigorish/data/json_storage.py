"""Functions for reading and writing files."""
import json

from vigorish.enums import DataSet, LocalFileTask, S3FileTask, VigFile
from vigorish.util.sys_helpers import get_last_mod_time_utc
from vigorish.util.dt_format_strings import HTTP_TIME
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

    def save_json(self, data_set, parsed_data):
        local_filepath = None
        s3_object_key = None
        result_local = Result.Ok()
        result_s3 = Result.Ok()
        if self.json_stored_local_folder(VigFile.PARSED_JSON, data_set):
            result_local = self.save_json_local(data_set, parsed_data)
            if result_local.success:
                local_filepath = result_local.value
        if self.json_stored_s3(VigFile.PARSED_JSON, data_set):  # pragma: no cover
            result_s3 = self.save_json_s3(data_set, parsed_data)
            if result_s3.success:
                s3_object_key = result_s3.value
        result = Result.Combine([result_local, result_s3])
        if result.failure:
            return result
        return Result.Ok({"local_filepath": local_filepath, "s3_object_key": s3_object_key})

    def json_stored_local_folder(self, file_type, data_set):
        return self.file_helper.check_file_stored_local(file_type, data_set)

    def save_json_local(self, data_set, parsed_data):
        save_json_local_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.save_json_brooks_games_for_date_local_file,
            DataSet.BROOKS_PITCH_LOGS: self.save_json_brooks_pitch_logs_for_game_local_file,
            DataSet.BROOKS_PITCHFX: self.save_json_brooks_pitchfx_log_local_file,
            DataSet.BBREF_GAMES_FOR_DATE: self.save_json_bbref_games_for_date_local_file,
            DataSet.BBREF_BOXSCORES: self.save_json_bbref_boxscore_local_file,
        }
        return save_json_local_dict[data_set](parsed_data)

    def save_json_brooks_games_for_date_local_file(self, games_for_date):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            file_type=VigFile.PARSED_JSON,
            game_date=games_for_date.game_date,
            scraped_data=games_for_date,
        )

    def save_json_brooks_pitch_logs_for_game_local_file(self, pitch_logs_for_game):
        result = validate_brooks_game_id(pitch_logs_for_game.bb_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            file_type=VigFile.PARSED_JSON,
            game_date=game_dict["game_date"],
            scraped_data=pitch_logs_for_game,
            bb_game_id=pitch_logs_for_game.bb_game_id,
        )

    def save_json_brooks_pitchfx_log_local_file(self, pitchfx_log):
        result = validate_pitch_app_id(pitchfx_log.pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BROOKS_PITCHFX,
            file_type=VigFile.PARSED_JSON,
            game_date=game_dict["game_date"],
            scraped_data=pitchfx_log,
            pitch_app_id=pitchfx_log.pitch_app_id,
        )

    def save_json_bbref_games_for_date_local_file(self, games_for_date):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            file_type=VigFile.PARSED_JSON,
            game_date=games_for_date.game_date,
            scraped_data=games_for_date,
        )

    def save_json_bbref_boxscore_local_file(self, boxscore):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BBREF_BOXSCORES,
            file_type=VigFile.PARSED_JSON,
            game_date=boxscore.game_date,
            scraped_data=boxscore,
            bbref_game_id=boxscore.bbref_game_id,
        )

    def json_stored_s3(self, file_type, data_set):  # pragma: no cover
        return self.file_helper.check_file_stored_s3(file_type, data_set)

    def save_json_s3(self, data_set, parsed_data):  # pragma: no cover
        save_json_s3_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.save_json_brooks_games_for_date_s3,
            DataSet.BROOKS_PITCH_LOGS: self.save_json_brooks_pitch_logs_for_game_s3,
            DataSet.BROOKS_PITCHFX: self.save_json_brooks_pitchfx_log_s3,
            DataSet.BBREF_GAMES_FOR_DATE: self.save_json_bbref_games_for_date_s3,
            DataSet.BBREF_BOXSCORES: self.save_json_bbref_boxscore_s3,
        }
        return save_json_s3_dict[data_set](parsed_data)

    def save_json_brooks_games_for_date_s3(self, games_for_date):  # pragma: no cover
        return self.file_helper.perform_s3_task(
            task=S3FileTask.UPLOAD,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            file_type=VigFile.PARSED_JSON,
            game_date=games_for_date.game_date,
            scraped_data=games_for_date,
        )

    def save_json_brooks_pitch_logs_for_game_s3(self, pitch_logs_for_game):  # pragma: no cover
        result = validate_brooks_game_id(pitch_logs_for_game.bb_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.UPLOAD,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            file_type=VigFile.PARSED_JSON,
            game_date=game_dict["game_date"],
            scraped_data=pitch_logs_for_game,
            bb_game_id=pitch_logs_for_game.bb_game_id,
        )

    def save_json_brooks_pitchfx_log_s3(self, pitchfx_log):  # pragma: no cover
        result = validate_pitch_app_id(pitchfx_log.pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.UPLOAD,
            data_set=DataSet.BROOKS_PITCHFX,
            file_type=VigFile.PARSED_JSON,
            game_date=game_dict["game_date"],
            scraped_data=pitchfx_log,
            pitch_app_id=pitchfx_log.pitch_app_id,
        )

    def save_json_bbref_games_for_date_s3(self, games_for_date):  # pragma: no cover
        return self.file_helper.perform_s3_task(
            task=S3FileTask.UPLOAD,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            file_type=VigFile.PARSED_JSON,
            game_date=games_for_date.game_date,
            scraped_data=games_for_date,
        )

    def save_json_bbref_boxscore_s3(self, boxscore):  # pragma: no cover
        return self.file_helper.perform_s3_task(
            task=S3FileTask.UPLOAD,
            data_set=DataSet.BBREF_BOXSCORES,
            file_type=VigFile.PARSED_JSON,
            game_date=boxscore.game_date,
            scraped_data=boxscore,
            bbref_game_id=boxscore.bbref_game_id,
        )

    def save_patch_list(self, data_set, patch_list):
        local_filepath = None
        s3_object_key = None
        result_local = Result.Ok()
        result_s3 = Result.Ok()
        if self.json_stored_local_folder(VigFile.PATCH_LIST, data_set):
            result_local = self.save_patch_list_local(data_set, patch_list)
            if result_local.success:
                local_filepath = result_local.value
        if self.json_stored_s3(VigFile.PATCH_LIST, data_set):  # pragma: no cover
            result_s3 = self.save_patch_list_s3(data_set, patch_list)
            if result_s3.success:
                s3_object_key = result_s3.value
        result = Result.Combine([result_local, result_s3])
        if result.failure:
            return result
        return Result.Ok({"local_filepath": local_filepath, "s3_object_key": s3_object_key})

    def save_patch_list_local(self, data_set, patch_list):
        save_patch_list_local_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.save_brooks_games_for_date_patch_list_local_file,
            DataSet.BROOKS_PITCHFX: self.save_brooks_pitchfx_patch_list_local_file,
            DataSet.BBREF_GAMES_FOR_DATE: self.save_bbref_games_for_date_patch_list_local_file,
            DataSet.BBREF_BOXSCORES: self.save_bbref_boxscore_patch_list_local_file,
        }
        save_patch_list_for_data_set = save_patch_list_local_dict.get(data_set)
        return save_patch_list_for_data_set(patch_list) if save_patch_list_for_data_set else None

    def save_brooks_games_for_date_patch_list_local_file(self, patch_list):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            file_type=VigFile.PATCH_LIST,
            game_date=patch_list.game_date,
            scraped_data=patch_list,
        )

    def save_brooks_pitchfx_patch_list_local_file(self, patch_list):
        result = validate_bbref_game_id(patch_list.url_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BROOKS_PITCHFX,
            file_type=VigFile.PATCH_LIST,
            game_date=game_dict["game_date"],
            scraped_data=patch_list,
            bbref_game_id=patch_list.url_id,
        )

    def save_bbref_games_for_date_patch_list_local_file(self, patch_list):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            file_type=VigFile.PATCH_LIST,
            game_date=patch_list.game_date,
            scraped_data=patch_list,
        )

    def save_bbref_boxscore_patch_list_local_file(self, patch_list):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BBREF_BOXSCORES,
            file_type=VigFile.PATCH_LIST,
            game_date=patch_list.game_date,
            scraped_data=patch_list,
            pitch_app_id=patch_list.url_id,
        )

    def save_patch_list_s3(self, data_set, patch_list):  # pragma: no cover
        save_patch_list_s3_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.save_brooks_games_for_date_patch_list_s3,
            DataSet.BROOKS_PITCHFX: self.save_brooks_pitchfx_patch_list_s3,
            DataSet.BBREF_GAMES_FOR_DATE: self.save_bbref_games_for_date_patch_list_s3,
            DataSet.BBREF_BOXSCORES: self.save_bbref_boxscore_patch_list_s3,
        }
        save_patch_list_for_data_set = save_patch_list_s3_dict.get(data_set)
        return save_patch_list_for_data_set(patch_list) if save_patch_list_for_data_set else None

    def save_brooks_games_for_date_patch_list_s3(self, patch_list):  # pragma: no cover
        return self.file_helper.perform_s3_task(
            task=S3FileTask.UPLOAD,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            file_type=VigFile.PATCH_LIST,
            game_date=patch_list.game_date,
            scraped_data=patch_list,
        )

    def save_brooks_pitchfx_patch_list_s3(self, patch_list):  # pragma: no cover
        result = validate_bbref_game_id(patch_list.url_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.UPLOAD,
            data_set=DataSet.BROOKS_PITCHFX,
            file_type=VigFile.PATCH_LIST,
            game_date=game_dict["game_date"],
            scraped_data=patch_list,
            bbref_game_id=patch_list.url_id,
        )

    def save_bbref_games_for_date_patch_list_s3(self, patch_list):  # pragma: no cover
        return self.file_helper.perform_s3_task(
            task=S3FileTask.UPLOAD,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            file_type=VigFile.PATCH_LIST,
            game_date=patch_list.game_date,
            scraped_data=patch_list,
        )

    def save_bbref_boxscore_patch_list_s3(self, patch_list):  # pragma: no cover
        return self.file_helper.perform_s3_task(
            task=S3FileTask.UPLOAD,
            data_set=DataSet.BBREF_BOXSCORES,
            file_type=VigFile.PATCH_LIST,
            game_date=patch_list.game_date,
            scraped_data=patch_list,
            pitch_app_id=patch_list.url_id,
        )

    def save_combined_game_data(self, combined_data):
        local_filepath = None
        s3_object_key = None
        result_local = Result.Ok()
        result_s3 = Result.Ok()
        if self.json_stored_local_folder(VigFile.COMBINED_GAME_DATA, DataSet.ALL):
            result_local = self.save_combined_game_data_local_file(combined_data)
            if result_local.success:
                local_filepath = result_local.value
        if self.json_stored_s3(VigFile.COMBINED_GAME_DATA, DataSet.ALL):  # pragma: no cover
            result_s3 = self.save_combined_game_data_s3(combined_data)
            if result_s3.success:
                s3_object_key = result_s3.value
        result = Result.Combine([result_local, result_s3])
        if result.failure:
            return result
        return Result.Ok({"local_filepath": local_filepath, "s3_object_key": s3_object_key})

    def save_combined_game_data_local_file(self, combined_data):
        result = validate_bbref_game_id(combined_data["bbref_game_id"])
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.ALL,
            file_type=VigFile.COMBINED_GAME_DATA,
            game_date=game_dict["game_date"],
            scraped_data=combined_data,
            bbref_game_id=combined_data["bbref_game_id"],
        )

    def save_combined_game_data_s3(self, combined_data):  # pragma: no cover
        result = validate_bbref_game_id(combined_data["bbref_game_id"])
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.UPLOAD,
            data_set=DataSet.ALL,
            file_type=VigFile.COMBINED_GAME_DATA,
            game_date=game_dict["game_date"],
            scraped_data=combined_data,
            bbref_game_id=combined_data["bbref_game_id"],
        )

    def get_combined_game_data(self, bbref_game_id):
        if self.json_stored_local_folder(VigFile.COMBINED_GAME_DATA, DataSet.ALL):
            result = self.decode_combined_game_data_local_file(bbref_game_id)
            if result.success:
                return result.value
        if self.json_stored_s3(VigFile.COMBINED_GAME_DATA, DataSet.ALL):  # pragma: no cover
            result = self.decode_combined_game_data_s3(bbref_game_id)
            if result.success:
                return result.value
        return None

    def decode_combined_game_data_local_file(self, bbref_game_id):
        result = self.get_combined_game_data_local_file(bbref_game_id)
        if result.failure:
            return result
        filepath = result.value
        try:
            json_dict = json.loads(filepath.read_text())
            json_dict["last_modified"] = get_last_mod_time_utc(filepath).strftime(HTTP_TIME)
            return Result.Ok(json_dict)
        except Exception as e:
            error = f"Error: {repr(e)}"
            return Result.Fail(error)

    def get_combined_game_data_local_file(self, bbref_game_id):
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.ALL,
            file_type=VigFile.COMBINED_GAME_DATA,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def decode_combined_game_data_s3(self, bbref_game_id):  # pragma: no cover
        result = self.get_combined_game_data_s3(bbref_game_id)
        if result.failure:
            return result
        filepath = result.value
        try:
            json_dict = json.loads(filepath.read_text())
            json_dict["last_modified"] = get_last_mod_time_utc(filepath).strftime(HTTP_TIME)
            return Result.Ok(json_dict)
        except Exception as e:
            error = f"Error: {repr(e)}"
            return Result.Fail(error)

    def get_combined_game_data_s3(self, bbref_game_id):  # pragma: no cover
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DOWNLOAD,
            data_set=DataSet.ALL,
            file_type=VigFile.COMBINED_GAME_DATA,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def get_scraped_data(self, data_set, url_id):
        if self.json_stored_local_folder(VigFile.PARSED_JSON, data_set):
            result = self.get_scraped_data_local(data_set, url_id)
            if result.success:
                return result.value
        if self.json_stored_s3(VigFile.PARSED_JSON, data_set):  # pragma: no cover
            result = self.get_scraped_data_s3(data_set, url_id)
            if result.success:
                return result.value
        return None

    def get_scraped_data_local(self, data_set, url_id):
        get_scraped_data_local_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.decode_json_brooks_games_for_date_local_file,
            DataSet.BROOKS_PITCH_LOGS: self.decode_json_brooks_pitch_logs_for_game_local_file,
            DataSet.BROOKS_PITCHFX: self.decode_json_brooks_pitchfx_log_local_file,
            DataSet.BBREF_GAMES_FOR_DATE: self.decode_json_bbref_games_for_date_local_file,
            DataSet.BBREF_BOXSCORES: self.decode_json_bbref_boxscore_local_file,
        }
        return get_scraped_data_local_dict[data_set](url_id)

    def decode_json_brooks_games_for_date_local_file(self, game_date):
        result = self.get_json_brooks_games_for_date_local_file(game_date)
        if result.failure:
            return result
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DECODE_JSON,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            file_type=VigFile.PARSED_JSON,
            game_date=game_date,
            delete_file=False,
        )

    def get_json_brooks_games_for_date_local_file(self, game_date):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            file_type=VigFile.PARSED_JSON,
            game_date=game_date,
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
            file_type=VigFile.PARSED_JSON,
            game_date=game_dict["game_date"],
            bb_game_id=bb_game_id,
            delete_file=False,
        )

    def get_json_brooks_pitch_logs_for_game_local_file(self, bb_game_id):
        result = validate_brooks_game_id(bb_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            file_type=VigFile.PARSED_JSON,
            game_date=game_dict["game_date"],
            bb_game_id=bb_game_id,
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
            file_type=VigFile.PARSED_JSON,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
            delete_file=False,
        )

    def get_json_brooks_pitchfx_local_file(self, pitch_app_id):
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BROOKS_PITCHFX,
            file_type=VigFile.PARSED_JSON,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def decode_json_bbref_games_for_date_local_file(self, game_date):
        result = self.get_json_bbref_games_for_date_local_file(game_date)
        if result.failure:
            return result
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DECODE_JSON,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            file_type=VigFile.PARSED_JSON,
            game_date=game_date,
            delete_file=False,
        )

    def get_json_bbref_games_for_date_local_file(self, game_date):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            file_type=VigFile.PARSED_JSON,
            game_date=game_date,
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
            file_type=VigFile.PARSED_JSON,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
            delete_file=False,
        )

    def get_json_bbref_boxscore_local_file(self, bbref_game_id):
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BBREF_BOXSCORES,
            file_type=VigFile.PARSED_JSON,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def get_scraped_data_s3(self, data_set, url_id):  # pragma: no cover
        get_scraped_data_s3_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.decode_json_brooks_games_for_date_s3,
            DataSet.BROOKS_PITCH_LOGS: self.decode_json_brooks_pitch_logs_for_game_s3,
            DataSet.BROOKS_PITCHFX: self.decode_json_brooks_pitchfx_log_s3,
            DataSet.BBREF_GAMES_FOR_DATE: self.decode_json_bbref_games_for_date_s3,
            DataSet.BBREF_BOXSCORES: self.decode_json_bbref_boxscore_s3,
        }
        return get_scraped_data_s3_dict[data_set](url_id)

    def decode_json_brooks_games_for_date_s3(self, game_date):  # pragma: no cover
        result = self.get_json_brooks_games_for_date_s3(game_date)
        if result.failure:
            return result
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DECODE_JSON,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            file_type=VigFile.PARSED_JSON,
            game_date=game_date,
            delete_file=True,
        )

    def get_json_brooks_games_for_date_s3(self, game_date):  # pragma: no cover
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DOWNLOAD,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            file_type=VigFile.PARSED_JSON,
            game_date=game_date,
        )

    def decode_json_brooks_pitchfx_log_s3(self, pitch_app_id):  # pragma: no cover
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
            file_type=VigFile.PARSED_JSON,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
            delete_file=True,
        )

    def decode_json_brooks_pitch_logs_for_game_s3(self, bb_game_id):  # pragma: no cover
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
            file_type=VigFile.PARSED_JSON,
            game_date=game_dict["game_date"],
            bb_game_id=bb_game_id,
            delete_file=True,
        )

    def get_json_brooks_pitch_logs_for_game_s3(self, bb_game_id):  # pragma: no cover
        result = validate_brooks_game_id(bb_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DOWNLOAD,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            file_type=VigFile.PARSED_JSON,
            game_date=game_dict["game_date"],
            bb_game_id=bb_game_id,
        )

    def get_json_brooks_pitchfx_s3(self, pitch_app_id):  # pragma: no cover
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DOWNLOAD,
            data_set=DataSet.BROOKS_PITCHFX,
            file_type=VigFile.PARSED_JSON,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def decode_json_bbref_games_for_date_s3(self, game_date):  # pragma: no cover
        result = self.get_json_bbref_games_for_date_s3(game_date)
        if result.failure:
            return result
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DECODE_JSON,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            file_type=VigFile.PARSED_JSON,
            game_date=game_date,
            delete_file=True,
        )

    def get_json_bbref_games_for_date_s3(self, game_date):  # pragma: no cover
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DOWNLOAD,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            file_type=VigFile.PARSED_JSON,
            game_date=game_date,
        )

    def decode_json_bbref_boxscore_s3(self, bbref_game_id):  # pragma: no cover
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
            file_type=VigFile.PARSED_JSON,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
            delete_file=True,
        )

    def get_json_bbref_boxscore_s3(self, bbref_game_id):  # pragma: no cover
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DOWNLOAD,
            data_set=DataSet.BBREF_BOXSCORES,
            file_type=VigFile.PARSED_JSON,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def get_patch_list(self, data_set, url_id):
        if self.json_stored_local_folder(VigFile.PATCH_LIST, data_set):
            result = self.get_patch_list_local(data_set, url_id)
            if result.success:
                return result.value
        if self.json_stored_s3(VigFile.PATCH_LIST, data_set):  # pragma: no cover
            result = self.get_patch_list_s3(data_set, url_id)
            if result.success:
                return result.value
        return None

    def get_patch_list_local(self, data_set, url_id):
        get_patch_list_local_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.decode_brooks_games_for_date_patch_list_local_file,
            DataSet.BROOKS_PITCHFX: self.decode_brooks_pitchfx_patch_list_local_file,
            DataSet.BBREF_GAMES_FOR_DATE: self.decode_bbref_games_for_date_patch_list_local_file,
            DataSet.BBREF_BOXSCORES: self.decode_bbref_boxscore_patch_list_local_file,
        }
        get_patch_list_for_data_set = get_patch_list_local_dict.get(data_set)
        return get_patch_list_for_data_set(url_id) if get_patch_list_for_data_set else Result.Ok({})

    def decode_brooks_games_for_date_patch_list_local_file(self, game_date):
        result = self.get_brooks_games_for_date_patch_list_local_file(game_date)
        if result.failure:
            return result
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DECODE_JSON,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            file_type=VigFile.PATCH_LIST,
            game_date=game_date,
            delete_file=False,
        )

    def get_brooks_games_for_date_patch_list_local_file(self, game_date):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            file_type=VigFile.PATCH_LIST,
            game_date=game_date,
        )

    def decode_brooks_pitchfx_patch_list_local_file(self, bbref_game_id):
        result = self.get_brooks_pitchfx_patch_list_local_file(bbref_game_id)
        if result.failure:
            return result
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DECODE_JSON,
            data_set=DataSet.BROOKS_PITCHFX,
            file_type=VigFile.PATCH_LIST,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
            delete_file=False,
        )

    def get_brooks_pitchfx_patch_list_local_file(self, bbref_game_id):
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BROOKS_PITCHFX,
            file_type=VigFile.PATCH_LIST,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def decode_bbref_games_for_date_patch_list_local_file(self, game_date):
        result = self.get_bbref_games_for_date_patch_list_local_file(game_date)
        if result.failure:
            return result
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DECODE_JSON,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            file_type=VigFile.PATCH_LIST,
            game_date=game_date,
            delete_file=False,
        )

    def get_bbref_games_for_date_patch_list_local_file(self, game_date):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            file_type=VigFile.PATCH_LIST,
            game_date=game_date,
        )

    def decode_bbref_boxscore_patch_list_local_file(self, bbref_game_id):
        result = self.get_bbref_boxscore_patch_list_local_file(bbref_game_id)
        if result.failure:
            return result
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DECODE_JSON,
            data_set=DataSet.BBREF_BOXSCORES,
            file_type=VigFile.PATCH_LIST,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
            delete_file=False,
        )

    def get_bbref_boxscore_patch_list_local_file(self, bbref_game_id):
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BBREF_BOXSCORES,
            file_type=VigFile.PATCH_LIST,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def get_patch_list_s3(self, data_set, url_id):  # pragma: no cover
        get_patch_list_s3_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.decode_brooks_games_for_date_patch_list_s3,
            DataSet.BROOKS_PITCHFX: self.decode_brooks_pitchfx_patch_list_s3,
            DataSet.BBREF_GAMES_FOR_DATE: self.decode_bbref_games_for_date_patch_list_s3,
            DataSet.BBREF_BOXSCORES: self.decode_bbref_boxscore_patch_list_s3,
        }
        get_patch_list_for_data_set = get_patch_list_s3_dict.get(data_set)
        return get_patch_list_for_data_set(url_id) if get_patch_list_for_data_set else None

    def decode_brooks_games_for_date_patch_list_s3(self, game_date):  # pragma: no cover
        result = self.get_brooks_games_for_date_patch_list_s3(game_date)
        if result.failure:
            return result
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DECODE_JSON,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            file_type=VigFile.PATCH_LIST,
            game_date=game_date,
            delete_file=True,
        )

    def get_brooks_games_for_date_patch_list_s3(self, game_date):  # pragma: no cover
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DOWNLOAD,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            file_type=VigFile.PATCH_LIST,
            game_date=game_date,
        )

    def decode_brooks_pitchfx_patch_list_s3(self, bbref_game_id):  # pragma: no cover
        result = self.get_brooks_pitchfx_patch_list_local_file(bbref_game_id)
        if result.failure:
            return result
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DECODE_JSON,
            data_set=DataSet.BROOKS_PITCHFX,
            file_type=VigFile.PATCH_LIST,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
            delete_file=True,
        )

    def get_brooks_pitchfx_patch_list_s3(self, bbref_game_id):  # pragma: no cover
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DOWNLOAD,
            data_set=DataSet.BROOKS_PITCHFX,
            file_type=VigFile.PATCH_LIST,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def decode_bbref_games_for_date_patch_list_s3(self, game_date):  # pragma: no cover
        result = self.get_bbref_games_for_date_patch_list_s3(game_date)
        if result.failure:
            return result
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DECODE_JSON,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            file_type=VigFile.PATCH_LIST,
            game_date=game_date,
            delete_file=True,
        )

    def get_bbref_games_for_date_patch_list_s3(self, game_date):  # pragma: no cover
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DOWNLOAD,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            file_type=VigFile.PATCH_LIST,
            game_date=game_date,
        )

    def decode_bbref_boxscore_patch_list_s3(self, bbref_game_id):  # pragma: no cover
        result = self.get_bbref_boxscore_patch_list_s3(bbref_game_id)
        if result.failure:
            return result
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DECODE_JSON,
            data_set=DataSet.BBREF_BOXSCORES,
            file_type=VigFile.PATCH_LIST,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
            delete_file=True,
        )

    def get_bbref_boxscore_patch_list_s3(self, bbref_game_id):  # pragma: no cover
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DOWNLOAD,
            data_set=DataSet.BBREF_BOXSCORES,
            file_type=VigFile.PATCH_LIST,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def delete_brooks_games_for_date_local_file(self, game_date):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DELETE_FILE,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            file_type=VigFile.PARSED_JSON,
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
            file_type=VigFile.PARSED_JSON,
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
            file_type=VigFile.PARSED_JSON,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def delete_bbref_games_for_date_local_file(self, game_date):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DELETE_FILE,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            file_type=VigFile.PARSED_JSON,
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
            file_type=VigFile.PARSED_JSON,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def delete_brooks_games_for_date_s3(self, game_date):  # pragma: no cover
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DELETE,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            file_type=VigFile.PARSED_JSON,
            game_date=game_date,
        )

    def delete_brooks_pitch_logs_for_game_s3(self, bb_game_id):  # pragma: no cover
        result = validate_brooks_game_id(bb_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DELETE,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            file_type=VigFile.PARSED_JSON,
            game_date=game_dict["game_date"],
            bb_game_id=bb_game_id,
        )

    def delete_brooks_pitchfx_log_s3(self, pitch_app_id):  # pragma: no cover
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DELETE,
            data_set=DataSet.BROOKS_PITCHFX,
            file_type=VigFile.PARSED_JSON,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def delete_bbref_games_for_date_s3(self, game_date):  # pragma: no cover
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DELETE,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            file_type=VigFile.PARSED_JSON,
            game_date=game_date,
        )

    def delete_bbref_boxscore_s3(self, bbref_game_id):  # pragma: no cover
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DELETE,
            data_set=DataSet.BBREF_BOXSCORES,
            file_type=VigFile.PARSED_JSON,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def rename_brooks_pitchfx_log(self, old_pitch_app_id, new_pitch_app_id, year):  # pragma: no cover
        result = validate_pitch_app_id(old_pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        old_key = self.file_helper.get_object_key(
            file_type=VigFile.PARSED_JSON,
            data_set=DataSet.BROOKS_PITCHFX,
            game_date=game_dict["game_date"],
            pitch_app_id=old_pitch_app_id,
        )
        result = validate_pitch_app_id(new_pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        new_key = self.file_helper.get_object_key(
            file_type=VigFile.PARSED_JSON,
            data_set=DataSet.BROOKS_PITCHFX,
            game_date=game_dict["game_date"],
            pitch_app_id=new_pitch_app_id,
        )
        return self.file_helper.rename_s3_object(old_key, new_key)
