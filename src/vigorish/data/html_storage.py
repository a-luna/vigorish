"""Functions for reading and writing files."""
from pathlib import Path

from vigorish.enums import DataSet, DocFormat, LocalFileTask, S3FileTask
from vigorish.data.file_helper import FileHelper
from vigorish.util.dt_format_strings import DATE_ONLY
from vigorish.util.result import Result
from vigorish.util.string_helpers import validate_bbref_game_id, validate_pitch_app_id


class HtmlStorage:
    """Perform CRUD operations on HTML files stored locally and/or in S3."""

    def __init__(self, config, file_helper):
        self.config = config
        self.file_helper = file_helper

    def write_html_brooks_games_for_date_local_file(self, game_date, html):
        return self.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            doc_format=DocFormat.HTML,
            game_date=game_date,
            scraped_data=html,
        )

    def write_html_brooks_pitch_log_local_file(self, pitch_app_id, html):
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            doc_format=DocFormat.HTML,
            game_date=game_dict["game_date"],
            scraped_data=html,
            pitch_app_id=pitch_app_id,
        )

    def write_html_brooks_pitchfx_local_file(self, pitch_app_id, html):
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BROOKS_PITCHFX,
            doc_format=DocFormat.HTML,
            game_date=game_dict["game_date"],
            scraped_data=html,
            pitch_app_id=pitch_app_id,
        )

    def write_html_bbref_games_for_date_local_file(self, game_date, html):
        return self.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            doc_format=DocFormat.HTML,
            game_date=game_date,
            scraped_data=html,
        )

    def write_html_bbref_boxscore_local_file(self, bbref_game_id, html):
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BBREF_BOXSCORES,
            doc_format=DocFormat.HTML,
            game_date=game_dict["game_date"],
            scraped_data=html,
            bbref_game_id=bbref_game_id,
        )

    def get_html_brooks_games_for_date_local_file(self, game_date):
        return self.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            doc_format=DocFormat.HTML,
            game_date=game_date,
        )

    def get_html_brooks_pitch_log_local_file(self, pitch_app_id):
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            doc_format=DocFormat.HTML,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def get_html_brooks_pitchfx_local_file(self, pitch_app_id):
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BROOKS_PITCHFX,
            doc_format=DocFormat.HTML,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def get_html_bbref_games_for_date_local_file(self, game_date):
        return self.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            doc_format=DocFormat.HTML,
            game_date=game_date,
        )

    def get_html_bbref_boxscore_local_file(self, bbref_game_id):
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BBREF_BOXSCORES,
            doc_format=DocFormat.HTML,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def upload_html_brooks_games_for_date(self, game_date, html):
        result = write_html_brooks_games_for_date(game_date, html)
        if result.failure:
            return result
        return self.perform_task(
            task=S3FileTask.UPLOAD,
            doc_format=DocFormat.HTML,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            game_date=game_date,
        )

    def upload_html_brooks_pitch_log(self, pitch_app_id, html):
        result = write_html_brooks_pitch_log(pitch_app_id, html)
        if result.failure:
            return result
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=S3FileTask.UPLOAD,
            doc_format=DocFormat.HTML,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def upload_html_brooks_pitchfx(self, pitch_app_id, html):
        result = write_html_brooks_pitchfx(pitch_app_id, html)
        if result.failure:
            return result
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=S3FileTask.UPLOAD,
            doc_format=DocFormat.HTML,
            data_set=DataSet.BROOKS_PITCHFX,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def upload_html_bbref_games_for_date(self, game_date, html):
        result = write_html_bbref_games_for_date(game_date, html)
        if result.failure:
            return result
        return self.perform_task(
            task=S3FileTask.UPLOAD,
            doc_format=DocFormat.HTML,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            game_date=game_date,
        )

    def upload_html_bbref_boxscore(self, bbref_game_id, html):
        """Upload a file to S3 containing json encoded BBRefBoxscore object."""
        result = write_html_bbref_boxscore(bbref_game_id, html)
        if result.failure:
            return result
        filepath = result.value
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=S3FileTask.UPLOAD,
            doc_format=DocFormat.HTML,
            data_set=DataSet.BBREF_BOXSCORES,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def download_html_brooks_games_for_date(self, game_date):
        """Download raw HTML for brooks daily scoreboard page."""
        return self.perform_task(
            task=S3FileTask.DOWNLOAD,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            doc_format=DocFormat.HTML,
            game_date=game_date,
        )

    def download_html_brooks_pitch_log_page(self, pitch_app_id):
        """Download raw HTML for brooks pitch log page for a single pitching appearance."""
        result = validate_brooks_game_id(bb_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=S3FileTask.DOWNLOAD,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            doc_format=DocFormat.HTML,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def download_html_brooks_pitchfx_log(self, pitch_app_id):
        """Download raw HTML for brooks pitchfx data for a single pitching appearance."""
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=S3FileTask.DOWNLOAD,
            doc_format=DocFormat.HTML,
            data_set=DataSet.BROOKS_PITCHFX,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def download_html_bbref_games_for_date(self, game_date):
        """Download raw HTML for bbref daily scoreboard page."""
        return self.perform_task(
            task=S3FileTask.DOWNLOAD,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            doc_format=DocFormat.HTML,
            game_date=game_date,
        )

    def download_html_bbref_boxscore(self, bbref_game_id):
        """Download raw HTML for bbref daily scoreboard page."""
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.perform_task(
            task=S3FileTask.DOWNLOAD,
            doc_format=DocFormat.HTML,
            data_set=DataSet.BBREF_BOXSCORES,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )
