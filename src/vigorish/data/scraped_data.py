from dateutil import parser
from pathlib import Path

from vigorish.config.database import DateScrapeStatus, PitchAppearanceScrapeStatus
from vigorish.data.file_helper import FileHelper
from vigorish.data.html_storage import HtmlStorage
from vigorish.data.json_storage import JsonStorage
from vigorish.enums import DataSet, DocFormat
from vigorish.util.result import Result


class ScrapedData:
    def __init__(self, db, config):
        self.db = db
        self.config = config
        self.file_helper = FileHelper(config)
        self.html_storage = HtmlStorage(config, self.file_helper)
        self.json_storage = JsonStorage(config, self.file_helper)

    def save_html(self, data_set, url_id, html):
        return self.html_storage.save_html(data_set, url_id, html)

    def get_html(self, data_set, url_id):
        return self.html_storage.get_html(data_set, url_id)

    def save_json(self, data_set, parsed_data):
        return self.json_storage.save_json(data_set, parsed_data)

    def get_brooks_games_for_date(self, game_date):
        result = self.json_storage.decode_json_brooks_games_for_date_local_file(game_date)
        if result.success:
            return result
        return self.json_storage.decode_json_brooks_games_for_date_s3(game_date)

    def get_brooks_pitch_logs_for_game(self, bb_game_id):
        result = self.json_storage.decode_json_brooks_pitch_logs_for_game_local_file(bb_game_id)
        if result.success:
            return result
        return self.json_storage.decode_json_brooks_pitch_logs_for_game_s3(bb_game_id)

    def get_brooks_pitchfx_log(self, pitch_app_id):
        result = self.json_storage.decode_json_brooks_pitchfx_log_local_file(pitch_app_id)
        if result.success:
            return result
        return self.json_storage.decode_json_brooks_pitchfx_log_s3(pitch_app_id)

    def get_bbref_games_for_date(self, game_date):
        result = self.json_storage.decode_json_bbref_games_for_date_local_file(game_date)
        if result.success:
            return result
        return self.json_storage.decode_json_bbref_games_for_date_s3(game_date)

    def get_bbref_boxscore(self, bbref_game_id):
        result = self.json_storage.decode_json_bbref_boxscore_local_file(bbref_game_id)
        if result.success:
            return result
        return self.json_storage.decode_json_bbref_boxscore_s3(bbref_game_id)

    def get_all_brooks_pitch_logs_for_date(self, game_date):
        brooks_game_ids = DateScrapeStatus.get_all_brooks_game_ids_for_date(self.db, game_date)
        pitch_logs = []
        for game_id in brooks_game_ids:
            result = self.get_brooks_pitch_logs_for_game(game_id)
            if result.failure:
                continue
            pitch_logs.append(result.value)
        return Result.Ok(pitch_logs)

    def get_all_pitchfx_logs_for_game(self, bbref_game_id):
        p_app_ids = PitchAppearanceScrapeStatus.get_all_pitch_app_ids_for_game_with_pitchfx_data(
            self.db, bbref_game_id
        )
        fetch_tasks = [self.get_brooks_pitchfx_log(pitch_app_id) for pitch_app_id in p_app_ids]
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

    def get_all_brooks_dates_scraped(self, year):
        json_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.JSON, data_set=DataSet.BROOKS_GAMES_FOR_DATE, year=year
        )
        html_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.HTML, data_set=DataSet.BROOKS_GAMES_FOR_DATE, year=year
        )
        all_files = self.file_helper.bucket.objects.all()
        matching_files = [
            obj.key for obj in all_files if json_folder in obj.key and html_folder not in obj.key
        ]
        scraped_dates = [parser.parse(Path(filename).stem[-10:]) for filename in matching_files]
        return Result.Ok(scraped_dates)

    def get_all_scraped_brooks_game_ids(self, year):
        json_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.JSON, data_set=DataSet.BROOKS_PITCH_LOGS, year=year
        )
        html_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.HTML, data_set=DataSet.BROOKS_PITCH_LOGS, year=year
        )
        scraped_gameids = [
            Path(obj.key).stem
            for obj in self.file_helper.bucket.objects.all()
            if json_folder in obj.key and html_folder not in obj.key
        ]
        return Result.Ok(scraped_gameids)

    def get_all_bbref_dates_scraped(self, year):
        json_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.JSON, data_set=DataSet.BBREF_GAMES_FOR_DATE, year=year
        )
        html_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.HTML, data_set=DataSet.BBREF_GAMES_FOR_DATE, year=year
        )
        scraped_dates = [
            parser.parse(Path(obj.key).stem[-10:])
            for obj in self.file_helper.bucket.objects.all()
            if json_folder in obj.key and html_folder not in obj.key
        ]
        return Result.Ok(scraped_dates)

    def get_all_scraped_bbref_game_ids(self, year):
        json_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.JSON, data_set=DataSet.BBREF_BOXSCORES, year=year
        )
        html_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.HTML, data_set=DataSet.BBREF_BOXSCORES, year=year
        )
        scraped_game_ids = [
            Path(obj.key).stem
            for obj in self.file_helper.bucket.objects.all()
            if json_folder in obj.key and html_folder not in obj.key
        ]
        return Result.Ok(scraped_game_ids)

    def get_all_scraped_pitchfx_pitch_app_ids(self, year):
        json_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.JSON, data_set=DataSet.BROOKS_PITCHFX, year=year
        )
        html_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.HTML, data_set=DataSet.BROOKS_PITCHFX, year=year
        )
        scraped_pitch_app_ids = [
            Path(obj.key).stem
            for obj in self.file_helper.bucket.objects.all()
            if json_folder in obj.key and html_folder not in obj.key
        ]
        return Result.Ok(scraped_pitch_app_ids)
