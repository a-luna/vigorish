"""Functions for reading and writing files."""
import json
import os
from pathlib import Path
from string import Template

from vigorish.enums import DataSet, DocFormat, FileTask
from vigorish.scrape.json_decoder import (
    decode_brooks_games_for_date,
    decode_bbref_games_for_date,
    decode_bbref_boxscore,
    decode_brooks_pitch_logs_for_game,
    decode_brooks_pitchfx_log,
)
from vigorish.util.dt_format_strings import DATE_ONLY, DATE_ONLY_TABLE_ID
from vigorish.util.result import Result
from vigorish.util.string_helpers import (
    validate_bbref_game_id,
    validate_brooks_game_id,
    validate_pitch_app_id,
)


class LocalFileHelper:
    """Encode/decode JSON and retrieve local versions of scraped web pages."""

    def __init__(self, config_file):
        self.config_file = config_file

    @property
    def folderpath_dict(self):
        html_local_folder = self.config_file.all_settings.get("HTML_LOCAL_FOLDER_PATH")
        json_local_folder = self.config_file.all_settings.get("JSON_LOCAL_FOLDER_PATH")
        html_local_folder_dict = {
            data_set: html_local_folder.current_setting(data_set=data_set)
            for data_set in DataSet
            if data_set != DataSet.ALL
        }
        json_local_folder_dict = {
            data_set: json_local_folder.current_setting(data_set=data_set)
            for data_set in DataSet
            if data_set != DataSet.ALL
        }
        return {
            DocFormat.HTML: html_local_folder_dict,
            DocFormat.JSON: json_local_folder_dict,
        }

    @property
    def filename_dict(self):
        html_file_name_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.get_file_name_html_brooks_games_for_date,
            DataSet.BROOKS_PITCH_LOGS: self.get_file_name_html_brooks_pitch_log,
            DataSet.BROOKS_PITCHFX: self.get_file_name_html_brooks_pitchfx,
            DataSet.BBREF_GAMES_FOR_DATE: self.get_file_name_html_bbref_games_for_date,
            DataSet.BBREF_BOXSCORES: self.get_file_name_html_bbref_boxscore,
        }
        json_file_name_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.get_file_name_json_brooks_games_for_date,
            DataSet.BROOKS_PITCH_LOGS: self.get_file_name_json_brooks_pitch_log_for_game,
            DataSet.BROOKS_PITCHFX: self.get_file_name_json_brooks_pitchfx,
            DataSet.BBREF_GAMES_FOR_DATE: self.get_file_name_json_bbref_games_for_date,
            DataSet.BBREF_BOXSCORES: self.get_file_name_json_bbref_boxscore,
        }
        return {
            DocFormat.HTML: html_file_name_dict,
            DocFormat.JSON: json_file_name_dict,
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

    def write_html_brooks_games_for_date(self, game_date, html):
        return self.perform_task(
            task=FileTask.WRITE_FILE,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            doc_format=DocFormat.HTML,
            game_date=games_for_date.game_date,
            scraped_data=html,
        )

    def write_json_brooks_games_for_date(self, games_for_date):
        return self.perform_task(
            task=FileTask.WRITE_FILE,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=games_for_date.game_date,
            scraped_data=games_for_date,
        )

    def write_html_brooks_pitch_log(self, pitch_app_id, html):
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=FileTask.WRITE_FILE,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            doc_format=DocFormat.HTML,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def write_json_brooks_pitch_logs_for_game(self, pitch_logs_for_game):
        return self.perform_task(
            task=FileTask.WRITE_FILE,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            doc_format=DocFormat.JSON,
            game_date=pitch_logs_for_game.game_date,
            scraped_data=pitch_logs_for_game,
            bb_game_id=pitch_logs_for_game.bb_game_id,
        )

    def write_html_brooks_pitchfx(self, pitch_app_id):
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=FileTask.WRITE_FILE,
            data_set=DataSet.BROOKS_PITCHFX,
            doc_format=DocFormat.HTML,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def write_json_brooks_pitchfx_log(self, pitchfx_log):
        return self.perform_task(
            task=FileTask.WRITE_FILE,
            data_set=DataSet.BROOKS_PITCHFX,
            doc_format=DocFormat.JSON,
            game_date=pitchfx_log.game_date,
            scraped_data=pitchfx_log,
            pitch_app_id=pitchfx_log.pitch_app_id,
        )

    def write_html_bbref_games_for_date(self, game_date):
        return self.perform_task(
            task=FileTask.WRITE_FILE,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            doc_format=DocFormat.HTML,
            game_date=game_date,
        )

    def write_json_bbref_games_for_date(self, games_for_date):
        return self.perform_task(
            task=FileTask.WRITE_FILE,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=games_for_date.game_date,
            scraped_data=games_for_date,
        )

    def write_html_bbref_boxscore(self, bbref_game_id):
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=FileTask.WRITE_FILE,
            data_set=DataSet.BBREF_BOXSCORES,
            doc_format=DocFormat.HTML,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def write_json_bbref_boxscore(self, boxscore):
        return self.perform_task(
            task=FileTask.WRITE_FILE,
            data_set=DataSet.BBREF_BOXSCORES,
            doc_format=DocFormat.JSON,
            scraped_data=boxscore,
            game_date=boxscore.game_date,
            bbref_game_id=boxscore.bbref_game_id,
        )

    def get_html_brooks_games_for_date(self, game_date):
        return self.perform_task(
            task=FileTask.GET_FILE,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            doc_format=DocFormat.HTML,
            game_date=game_date,
        )

    def get_json_brooks_games_for_date(self, game_date):
        return self.perform_task(
            task=FileTask.GET_FILE,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=game_date,
        )

    def get_html_brooks_pitch_log(self, pitch_app_id):
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=FileTask.GET_FILE,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            doc_format=DocFormat.HTML,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def get_json_brooks_pitch_log_for_game(self, bb_game_id):
        result = validate_brooks_game_id(bb_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=FileTask.GET_FILE,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            bb_game_id=bb_game_id,
        )

    def get_html_brooks_pitchfx(self, pitch_app_id):
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=FileTask.GET_FILE,
            data_set=DataSet.BROOKS_PITCHFX,
            doc_format=DocFormat.HTML,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def get_json_brooks_pitchfx(self, pitch_app_id):
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=FileTask.GET_FILE,
            data_set=DataSet.BROOKS_PITCHFX,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def get_html_bbref_games_for_date(self, game_date):
        return self.perform_task(
            task=FileTask.GET_FILE,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            doc_format=DocFormat.HTML,
            game_date=game_date,
        )

    def get_json_bbref_games_for_date(self, game_date):
        return self.perform_task(
            task=FileTask.GET_FILE,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            doc_format=DocFormat.JSON,
            game_date=game_date,
        )

    def get_html_bbref_boxscore(self, bbref_game_id):
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=FileTask.GET_FILE,
            data_set=DataSet.BBREF_BOXSCORES,
            doc_format=DocFormat.HTML,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def get_json_bbref_boxscore(self, bbref_game_id):
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=FileTask.GET_FILE,
            data_set=DataSet.BBREF_BOXSCORES,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def decode_json_brooks_games_for_date(self, game_date, delete_file=False):
        """Decode BrooksGamesForDate object from json file."""
        return self.perform_task(
            task=FileTask.DECODE_JSON,
            doc_format=DocFormat.JSON,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            game_date=game_date,
            delete_file=delete_file,
        )

    def decode_json_brooks_pitch_logs_for_game(self, bb_game_id, delete_file=False):
        """Decode BrooksPitchLogsForGame object from file."""
        result = validate_brooks_game_id(bb_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=FileTask.DECODE_JSON,
            doc_format=DocFormat.JSON,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            game_date=game_dict["game_date"],
            bb_game_id=bb_game_id,
            delete_file=delete_file,
        )

    def decode_json_brooks_pitchfx_log(self, pitch_app_id, delete_file=False):
        """Decode BrooksPitchFxLog object from json file."""
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=FileTask.DECODE_JSON,
            data_set=DataSet.BROOKS_PITCHFX,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
            delete_file=delete_file,
        )

    def decode_json_bbref_games_for_date(self, game_date, delete_file=False):
        """Decode BBRefGamesForDate object from json file."""
        return self.perform_task(
            task=FileTask.DECODE_JSON,
            doc_format=DocFormat.JSON,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            game_date=game_date,
            delete_file=delete_file,
        )

    def decode_json_bbref_boxscore(self, bbref_game_id, delete_file=False):
        """Decode BBRefBoxscore object from file."""
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=FileTask.DECODE_JSON,
            data_set=DataSet.BBREF_BOXSCORES,
            doc_format=DocFormat.JSON,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
            delete_file=delete_file,
        )

    def perform_task(
        self,
        task,
        data_set,
        doc_format,
        game_date,
        scraped_data=None,
        bbref_game_id=None,
        bb_game_id=None,
        pitch_app_id=None,
        delete_file=True,
    ):
        filepath = self.get_local_file_path(
            data_set=data_set,
            doc_format=doc_format,
            game_date=game_date,
            bbref_game_id=bbref_game_id,
            bb_game_id=bb_game_id,
            pitch_app_id=pitch_app_id,
        )
        if task == FileTask.GET_FILE:
            return Path(filepath)

        if task == FileTask.WRITE_FILE:
            return self.write_to_file(scraped_data.as_json(), Path(filepath))

        if task == FileTask.DECODE_JSON:
            return self.decode_json(Path(filepath), data_set, delete_file)

    def get_local_file_path(
        self,
        data_set,
        doc_format,
        game_date,
        bbref_game_id=None,
        bb_game_id=None,
        pitch_app_id=None,
    ):
        folderpath = self.get_local_folder_path(
            year=game_date.year, data_set=data_set, doc_format=doc_format
        )
        filename = self.get_file_name(
            data_set=data_set,
            doc_format=doc_format,
            game_date=game_date,
            bbref_game_id=bbref_game_id,
            bb_game_id=bb_game_id,
            pitch_app_id=pitch_app_id,
        )
        return f"{folderpath}/{filename}"

    def get_local_folder_path(self, year, data_set, doc_format):
        return self.folderpath_dict[doc_format][data_set].resolve(year=year)

    def get_file_name(
        self, data_set, doc_format, game_date, bbref_game_id, bb_game_id, pitch_app_id
    ):
        identifier = self.get_file_identifier(
            game_date=game_date,
            bbref_game_id=bbref_game_id,
            bb_game_id=bb_game_id,
            pitch_app_id=pitch_app_id,
        )
        return self.filename_dict[doc_format][data_set](identifier)

    def get_file_identifier(self, game_date, bbref_game_id, bb_game_id, pitch_app_id):
        if pitch_app_id:
            return pitch_app_id
        elif bb_game_id:
            return bb_game_id
        elif bbref_game_id:
            return bbref_game_id
        elif game_date:
            return game_date
        else:
            raise ValueError(
                "Identifying value was not provided, unable to construct file name. (S3Helper.get_file_identifier)"
            )

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

    def write_to_file(self, data, filepath):
        """Write object in json format to file."""
        try:
            filepath.write_text(data)
            return Result.Ok(filepath)
        except Exception as e:
            error = f"Error: {repr(e)}"
            return Result.Fail(error)

    def decode_json(self, filepath, data_set, delete_file=True):
        try:
            contents = filepath.read_text()
            if delete_file:
                filepath.unlink()
            return self.json_decoder_dict[data_set](json.loads(contents))
        except Exception as e:
            error = f"Error: {repr(e)}"
            return Result.Fail(error)
