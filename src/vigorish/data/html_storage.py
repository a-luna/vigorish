"""Functions for reading and writing files."""
from vigorish.enums import DataSet, LocalFileTask, S3FileTask, VigFile
from vigorish.util.result import Result
from vigorish.util.string_helpers import validate_bbref_game_id, validate_pitch_app_id


class HtmlStorage:
    """Perform CRUD operations on HTML files stored locally and/or in S3."""

    def __init__(self, config, file_helper):
        self.config = config
        self.file_helper = file_helper

    def save_html(self, data_set, url_id, html):
        local_filepath = None
        s3_object_key = None
        result_local = Result.Ok()
        result_s3 = Result.Ok()
        if self.html_stored_local(data_set):
            result_local = self.save_html_local(data_set, url_id, html)
            if result_local.success:
                local_filepath = result_local.value
        if self.html_stored_s3(data_set):  # pragma: no cover
            result_s3 = self.save_html_s3(data_set, url_id, html)
            if result_s3.success:
                s3_object_key = result_s3.value
        result = Result.Combine([result_local, result_s3])
        if result.failure:
            return result
        return Result.Ok({"local_filepath": local_filepath, "s3_object_key": s3_object_key})

    def html_stored_local(self, data_set):
        return self.file_helper.check_file_stored_local(VigFile.SCRAPED_HTML, data_set)

    def save_html_local(self, data_set, url_id, html):
        save_html_local_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.save_html_brooks_games_for_date_local_file,
            DataSet.BROOKS_PITCH_LOGS: self.save_html_brooks_pitch_log_local_file,
            DataSet.BROOKS_PITCHFX: self.save_html_brooks_pitchfx_local_file,
            DataSet.BBREF_GAMES_FOR_DATE: self.save_html_bbref_games_for_date_local_file,
            DataSet.BBREF_BOXSCORES: self.save_html_bbref_boxscore_local_file,
        }
        return save_html_local_dict[data_set](url_id, html)

    def html_stored_s3(self, data_set):  # pragma: no cover
        return self.file_helper.check_file_stored_s3(VigFile.SCRAPED_HTML, data_set)

    def save_html_s3(self, data_set, url_id, html):  # pragma: no cover
        save_html_s3_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.upload_html_brooks_games_for_date,
            DataSet.BROOKS_PITCH_LOGS: self.upload_html_brooks_pitch_log,
            DataSet.BROOKS_PITCHFX: self.upload_html_brooks_pitchfx,
            DataSet.BBREF_GAMES_FOR_DATE: self.upload_html_bbref_games_for_date,
            DataSet.BBREF_BOXSCORES: self.upload_html_bbref_boxscore,
        }
        return save_html_s3_dict[data_set](url_id, html)

    def get_html(self, data_set, url_id):
        if self.html_stored_local(data_set):
            result_local = self.get_html_local(data_set, url_id)
            if result_local.success:
                return result_local.value
        if self.html_stored_s3(data_set):  # pragma: no cover
            result_s3 = self.get_html_s3(data_set, url_id)
            if result_s3.success:
                return result_s3.value
        return None

    def get_html_local(self, data_set, url_id):
        get_html_local_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.get_html_brooks_games_for_date_local_file,
            DataSet.BROOKS_PITCH_LOGS: self.get_html_brooks_pitch_log_local_file,
            DataSet.BROOKS_PITCHFX: self.get_html_brooks_pitchfx_local_file,
            DataSet.BBREF_GAMES_FOR_DATE: self.get_html_bbref_games_for_date_local_file,
            DataSet.BBREF_BOXSCORES: self.get_html_bbref_boxscore_local_file,
        }
        return get_html_local_dict[data_set](url_id)

    def get_html_s3(self, data_set, url_id):  # pragma: no cover
        get_html_s3_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.download_html_brooks_games_for_date,
            DataSet.BROOKS_PITCH_LOGS: self.download_html_brooks_pitch_log_page,
            DataSet.BROOKS_PITCHFX: self.download_html_brooks_pitchfx_log,
            DataSet.BBREF_GAMES_FOR_DATE: self.download_html_bbref_games_for_date,
            DataSet.BBREF_BOXSCORES: self.download_html_bbref_boxscore,
        }
        return get_html_s3_dict[data_set](url_id)

    def delete_html(self, data_set, url_id):
        result_local = Result.Ok()
        result_s3 = Result.Ok()
        if self.html_stored_local(data_set):
            result_local = self.delete_html_local(data_set, url_id)
        if self.html_stored_s3(data_set):  # pragma: no cover
            result_s3 = self.delete_html_s3(data_set, url_id)
        return Result.Combine([result_local, result_s3])

    def delete_html_local(self, data_set, url_id):
        delete_html_local_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.delete_html_brooks_games_for_date_local_file,
            DataSet.BROOKS_PITCH_LOGS: self.delete_html_brooks_pitch_logs_local_file,
            DataSet.BROOKS_PITCHFX: self.delete_html_brooks_pitchfx_log_local_file,
            DataSet.BBREF_GAMES_FOR_DATE: self.delete_html_bbref_games_for_date_local_file,
            DataSet.BBREF_BOXSCORES: self.delete_html_bbref_boxscore_local_file,
        }
        return delete_html_local_dict[data_set](url_id)

    def delete_html_s3(self, data_set, url_id):  # pragma: no cover
        delete_html_s3_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.delete_html_brooks_games_for_date_s3,
            DataSet.BROOKS_PITCH_LOGS: self.delete_html_brooks_pitch_logs_s3,
            DataSet.BROOKS_PITCHFX: self.delete_html_brooks_pitchfx_log_s3,
            DataSet.BBREF_GAMES_FOR_DATE: self.delete_html_bbref_games_for_date_s3,
            DataSet.BBREF_BOXSCORES: self.delete_html_bbref_boxscore_s3,
        }
        return delete_html_s3_dict[data_set](url_id)

    def save_html_brooks_games_for_date_local_file(self, game_date, html):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_date,
            scraped_data=html,
        )

    def save_html_brooks_pitch_log_local_file(self, pitch_app_id, html):
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_dict["game_date"],
            scraped_data=html,
            pitch_app_id=pitch_app_id,
        )

    def save_html_brooks_pitchfx_local_file(self, pitch_app_id, html):
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BROOKS_PITCHFX,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_dict["game_date"],
            scraped_data=html,
            pitch_app_id=pitch_app_id,
        )

    def save_html_bbref_games_for_date_local_file(self, game_date, html):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_date,
            scraped_data=html,
        )

    def save_html_bbref_boxscore_local_file(self, bbref_game_id, html):
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.WRITE_FILE,
            data_set=DataSet.BBREF_BOXSCORES,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_dict["game_date"],
            scraped_data=html,
            bbref_game_id=bbref_game_id,
        )

    def get_html_brooks_games_for_date_local_file(self, game_date):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_date,
        )

    def get_html_brooks_pitch_log_local_file(self, pitch_app_id):
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def get_html_brooks_pitchfx_local_file(self, pitch_app_id):
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BROOKS_PITCHFX,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def get_html_bbref_games_for_date_local_file(self, game_date):
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_date,
        )

    def get_html_bbref_boxscore_local_file(self, bbref_game_id):
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.READ_FILE,
            data_set=DataSet.BBREF_BOXSCORES,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def upload_html_brooks_games_for_date(self, game_date, html):  # pragma: no cover
        result = self.save_html_local(DataSet.BROOKS_GAMES_FOR_DATE, game_date, html)
        if result.failure:
            return result
        return self.file_helper.perform_s3_task(
            task=S3FileTask.UPLOAD,
            file_type=VigFile.SCRAPED_HTML,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            game_date=game_date,
        )

    def upload_html_brooks_pitch_log(self, pitch_app_id, html):  # pragma: no cover
        result = self.save_html_local(DataSet.BROOKS_PITCH_LOGS, pitch_app_id, html)
        if result.failure:
            return result
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.UPLOAD,
            file_type=VigFile.SCRAPED_HTML,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def upload_html_brooks_pitchfx(self, pitch_app_id, html):  # pragma: no cover
        result = self.save_html_local(DataSet.BROOKS_PITCHFX, pitch_app_id, html)
        if result.failure:
            return result
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.UPLOAD,
            file_type=VigFile.SCRAPED_HTML,
            data_set=DataSet.BROOKS_PITCHFX,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def upload_html_bbref_games_for_date(self, game_date, html):  # pragma: no cover
        result = self.save_html_local(DataSet.BBREF_GAMES_FOR_DATE, game_date, html)
        if result.failure:
            return result
        return self.file_helper.perform_s3_task(
            task=S3FileTask.UPLOAD,
            file_type=VigFile.SCRAPED_HTML,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            game_date=game_date,
        )

    def upload_html_bbref_boxscore(self, bbref_game_id, html):  # pragma: no cover
        result = self.save_html_local(DataSet.BBREF_BOXSCORES, bbref_game_id, html)
        if result.failure:
            return result
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.UPLOAD,
            file_type=VigFile.SCRAPED_HTML,
            data_set=DataSet.BBREF_BOXSCORES,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def download_html_brooks_games_for_date(self, game_date):  # pragma: no cover
        """Download raw HTML for brooks daily scoreboard page."""
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DOWNLOAD,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_date,
        )

    def download_html_brooks_pitch_log_page(self, pitch_app_id):  # pragma: no cover
        """Download raw HTML for brooks pitch log page for a single pitching appearance."""
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DOWNLOAD,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def download_html_brooks_pitchfx_log(self, pitch_app_id):  # pragma: no cover
        """Download raw HTML for brooks pitchfx data for a single pitching appearance."""
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DOWNLOAD,
            file_type=VigFile.SCRAPED_HTML,
            data_set=DataSet.BROOKS_PITCHFX,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def download_html_bbref_games_for_date(self, game_date):  # pragma: no cover
        """Download raw HTML for bbref daily scoreboard page."""
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DOWNLOAD,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_date,
        )

    def download_html_bbref_boxscore(self, bbref_game_id):  # pragma: no cover
        """Download raw HTML for bbref daily scoreboard page."""
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DOWNLOAD,
            file_type=VigFile.SCRAPED_HTML,
            data_set=DataSet.BBREF_BOXSCORES,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def delete_html_brooks_games_for_date_local_file(self, game_date):
        """Delete scraped HTML for brooks daily scoreboard page from local folder."""
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DELETE_FILE,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_date,
        )

    def delete_html_brooks_games_for_date_s3(self, game_date):  # pragma: no cover
        """Delete scraped HTML for brooks daily scoreboard page from S3."""
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DELETE,
            data_set=DataSet.BROOKS_GAMES_FOR_DATE,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_date,
        )

    def delete_html_bbref_games_for_date_local_file(self, game_date):
        """Delete scraped HTML for bbref daily scoreboard page from local folder."""
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DELETE_FILE,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_date,
        )

    def delete_html_bbref_games_for_date_s3(self, game_date):  # pragma: no cover
        """Delete scraped HTML for bbref daily scoreboard page from S3."""
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DELETE,
            data_set=DataSet.BBREF_GAMES_FOR_DATE,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_date,
        )

    def delete_html_bbref_boxscore_local_file(self, bbref_game_id):
        """Delete scraped HTML for bbref boxscore page from local folder."""
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DELETE_FILE,
            data_set=DataSet.BBREF_BOXSCORES,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def delete_html_bbref_boxscore_s3(self, bbref_game_id):  # pragma: no cover
        """Delete scraped HTML for bbref boxscore page from S3."""
        result = validate_bbref_game_id(bbref_game_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DELETE,
            data_set=DataSet.BBREF_BOXSCORES,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_dict["game_date"],
            bbref_game_id=bbref_game_id,
        )

    def delete_html_brooks_pitch_logs_local_file(self, pitch_app_id):
        """Delete scraped HTML for brooks pitch log page from local folder."""
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DELETE_FILE,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def delete_html_brooks_pitch_logs_s3(self, pitch_app_id):  # pragma: no cover
        """Delete scraped HTML for brooks pitch log page from S3."""
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DELETE,
            data_set=DataSet.BROOKS_PITCH_LOGS,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def delete_html_brooks_pitchfx_log_local_file(self, pitch_app_id):
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_local_file_task(
            task=LocalFileTask.DELETE_FILE,
            data_set=DataSet.BROOKS_PITCHFX,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )

    def delete_html_brooks_pitchfx_log_s3(self, pitch_app_id):  # pragma: no cover
        """Delete scraped HTML for brooks pitchfx data from s3."""
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        return self.file_helper.perform_s3_task(
            task=S3FileTask.DELETE,
            data_set=DataSet.BROOKS_PITCHFX,
            file_type=VigFile.SCRAPED_HTML,
            game_date=game_dict["game_date"],
            pitch_app_id=pitch_app_id,
        )
