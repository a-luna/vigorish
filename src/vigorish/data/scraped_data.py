from datetime import datetime
from dateutil import parser
from pathlib import Path

from vigorish.config.database import DateScrapeStatus, PitchAppScrapeStatus, Season
from vigorish.data.file_helper import FileHelper
from vigorish.data.html_storage import HtmlStorage
from vigorish.data.json_storage import JsonStorage
from vigorish.data.process.avg_time_between_pitches import calc_avg_time_between_pitches
from vigorish.data.process.combine_scraped_data import CombineScrapedData
from vigorish.enums import DataSet, DocFormat
from vigorish.util.numeric_helpers import trim_data_set
from vigorish.util.regex import URL_ID_REGEX, URL_ID_CONVERT_REGEX
from vigorish.util.result import Result


class ScrapedData:
    def __init__(self, db_session, config):
        self.db_session = db_session
        self.config = config
        self.file_helper = FileHelper(config)
        self.html_storage = HtmlStorage(config, self.file_helper)
        self.json_storage = JsonStorage(config, self.file_helper)
        self.combine_data = CombineScrapedData(db_session)

    def get_local_folderpath(self, doc_format, data_set, year):
        return self.file_helper.local_folderpath_dict[doc_format][data_set].resolve(year=year)

    def check_s3_bucket(self):
        return self.file_helper.check_s3_bucket()

    def create_all_folderpaths(self, year):
        return self.file_helper.create_all_folderpaths(year)

    def html_stored_s3(self, data_set):
        return self.html_storage.html_stored_s3(data_set)

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

    def save_json_combined_data(self, combined_data):
        return self.json_storage.save_json_combined_data(combined_data)

    def get_json_combined_data(self, bbref_game_id):
        return self.json_storage.get_json_combined_data(bbref_game_id)

    def get_all_brooks_pitch_logs_for_date(self, game_date):
        brooks_game_ids = DateScrapeStatus.get_all_brooks_game_ids_for_date(
            self.db_session, game_date
        )
        pitch_logs = []
        for game_id in brooks_game_ids:
            result = self.get_brooks_pitch_logs_for_game(game_id)
            if result.failure:
                continue
            pitch_logs.append(result.value)
        return Result.Ok(pitch_logs)

    def get_all_pitchfx_logs_for_game(self, bbref_game_id):
        p_app_ids = PitchAppScrapeStatus.get_all_scraped_pitch_app_ids_for_game_with_pitchfx_data(
            self.db_session, bbref_game_id
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

    def get_all_brooks_dates_scraped_html(self, year):
        html_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.HTML, data_set=DataSet.BROOKS_GAMES_FOR_DATE, year=year
        )
        scraped_dates = [
            Path(obj.key).stem
            for obj in self.file_helper.get_all_object_keys_in_s3_bucket()
            if html_folder in obj.key
        ]
        return scraped_dates

    def get_all_brooks_dates_scraped(self, year):
        json_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.JSON, data_set=DataSet.BROOKS_GAMES_FOR_DATE, year=year
        )
        html_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.HTML, data_set=DataSet.BROOKS_GAMES_FOR_DATE, year=year
        )
        scraped_dates = [
            parser.parse(Path(obj.key).stem[-10:])
            for obj in self.file_helper.get_all_object_keys_in_s3_bucket()
            if json_folder in obj.key and html_folder not in obj.key
        ]
        return Result.Ok(scraped_dates)

    def get_all_brooks_pitch_logs_scraped_html(self, year):
        html_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.HTML, data_set=DataSet.BROOKS_PITCH_LOGS, year=year
        )
        scraped_gameids = [
            Path(obj.key).stem
            for obj in self.file_helper.get_all_object_keys_in_s3_bucket()
            if html_folder in obj.key
        ]
        return scraped_gameids

    def get_all_scraped_brooks_game_ids(self, year):
        json_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.JSON, data_set=DataSet.BROOKS_PITCH_LOGS, year=year
        )
        html_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.HTML, data_set=DataSet.BROOKS_PITCH_LOGS, year=year
        )
        scraped_gameids = [
            Path(obj.key).stem
            for obj in self.file_helper.get_all_object_keys_in_s3_bucket()
            if json_folder in obj.key and html_folder not in obj.key
        ]
        return Result.Ok(scraped_gameids)

    def get_all_pitchfx_pitch_app_ids_scraped_html(self, year):
        html_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.HTML, data_set=DataSet.BROOKS_PITCHFX, year=year
        )
        scraped_pitch_app_ids = [
            Path(obj.key).stem
            for obj in self.file_helper.get_all_object_keys_in_s3_bucket()
            if html_folder in obj.key
        ]
        return scraped_pitch_app_ids

    def get_all_scraped_pitchfx_pitch_app_ids(self, year):
        json_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.JSON, data_set=DataSet.BROOKS_PITCHFX, year=year
        )
        html_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.HTML, data_set=DataSet.BROOKS_PITCHFX, year=year
        )
        scraped_pitch_app_ids = [
            Path(obj.key).stem
            for obj in self.file_helper.get_all_object_keys_in_s3_bucket()
            if json_folder in obj.key and html_folder not in obj.key
        ]
        return scraped_pitch_app_ids

    def get_all_bbref_dates_scraped_html(self, year):
        html_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.HTML, data_set=DataSet.BBREF_GAMES_FOR_DATE, year=year
        )
        scraped_dates = [
            Path(obj.key).stem
            for obj in self.file_helper.get_all_object_keys_in_s3_bucket()
            if html_folder in obj.key
        ]
        return scraped_dates

    def get_all_bbref_dates_scraped(self, year):
        json_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.JSON, data_set=DataSet.BBREF_GAMES_FOR_DATE, year=year
        )
        html_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.HTML, data_set=DataSet.BBREF_GAMES_FOR_DATE, year=year
        )
        scraped_dates = [
            parser.parse(Path(obj.key).stem[-10:])
            for obj in self.file_helper.get_all_object_keys_in_s3_bucket()
            if json_folder in obj.key and html_folder not in obj.key
        ]
        return Result.Ok(scraped_dates)

    def get_all_bbref_game_ids_scraped_html(self, year):
        html_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.HTML, data_set=DataSet.BBREF_BOXSCORES, year=year
        )
        scraped_game_ids = [
            Path(obj.key).stem
            for obj in self.file_helper.get_all_object_keys_in_s3_bucket()
            if html_folder in obj.key
        ]
        return scraped_game_ids

    def get_all_scraped_bbref_game_ids(self, year):
        json_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.JSON, data_set=DataSet.BBREF_BOXSCORES, year=year
        )
        html_folder = self.file_helper.get_s3_folderpath(
            doc_format=DocFormat.HTML, data_set=DataSet.BBREF_BOXSCORES, year=year
        )
        scraped_game_ids = [
            Path(obj.key).stem
            for obj in self.file_helper.get_all_object_keys_in_s3_bucket()
            if json_folder in obj.key and html_folder not in obj.key
        ]
        return Result.Ok(scraped_game_ids)

    def get_audit_report(self):
        scraped = self.get_all_bbref_game_ids_eligible_for_audit()
        successful = self.get_all_bbref_game_ids_audit_successful()
        failed = self.get_all_bbref_game_ids_audit_failed()
        data_error = self.get_all_bbref_game_ids_pitchfx_data_error()
        mlb_seasons = list(
            set(
                list(scraped.keys())
                + list(successful.keys())
                + list(failed.keys())
                + list(data_error.keys())
            )
        )
        return {
            year: {
                "scraped": scraped[year],
                "successful": successful[year],
                "failed": failed[year],
                "data_error": data_error[year],
            }
            for year in mlb_seasons
            if scraped[year] or successful[year] or failed[year] or data_error[year]
        }

    def get_all_bbref_game_ids_eligible_for_audit(self):
        return {
            s.year: s.get_all_bbref_game_ids_eligible_for_audit()
            for s in Season.all_regular_seasons(self.db_session)
        }

    def get_all_bbref_game_ids_audit_successful(self):
        return {
            s.year: s.get_all_bbref_game_ids_audit_successful()
            for s in Season.all_regular_seasons(self.db_session)
        }

    def get_all_bbref_game_ids_audit_failed(self):
        return {
            s.year: s.get_all_bbref_game_ids_audit_failed()
            for s in Season.all_regular_seasons(self.db_session)
        }

    def get_all_bbref_game_ids_pitchfx_data_error(self):
        return {
            s.year: s.get_all_bbref_game_ids_pitchfx_data_error()
            for s in Season.all_regular_seasons(self.db_session)
        }

    def get_scraped_ids_from_local_folder(self, doc_format, data_set, year):
        folderpath = self.get_local_folderpath(doc_format, data_set, year)
        url_ids = [file.stem for file in Path(folderpath).glob("*.*")]
        return self.validate_url_ids(doc_format, data_set, url_ids)

    def get_scraped_ids_from_s3(self, doc_format, data_set, year):
        scraped_html_s3_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.get_all_brooks_dates_scraped_html,
            DataSet.BROOKS_PITCH_LOGS: self.get_all_brooks_pitch_logs_scraped_html,
            DataSet.BROOKS_PITCHFX: self.get_all_pitchfx_pitch_app_ids_scraped_html,
            DataSet.BBREF_GAMES_FOR_DATE: self.get_all_bbref_dates_scraped_html,
            DataSet.BBREF_BOXSCORES: self.get_all_bbref_game_ids_scraped_html,
        }
        url_ids = scraped_html_s3_dict[data_set](year)
        return self.validate_url_ids(doc_format, data_set, url_ids)

    def validate_url_ids(self, doc_format, data_set, url_ids):
        url_ids = [uid for uid in url_ids if URL_ID_REGEX[doc_format][data_set].search(uid)]
        url_ids = self.convert_url_ids(doc_format, data_set, url_ids)
        return sorted(url_ids)

    def convert_url_ids(self, doc_format, data_set, url_ids):
        if data_set not in URL_ID_CONVERT_REGEX[doc_format]:
            return url_ids
        convert_regex = URL_ID_CONVERT_REGEX[doc_format][data_set]
        converted_url_ids = []
        for url_id in url_ids:
            match = convert_regex.search(url_id)
            if not match:
                raise ValueError(f"URL identifier is invalid: {url_id} ({data_set})")
            captured = match.groupdict()
            try:
                year = int(captured["year"])
                month = int(captured["month"])
                day = int(captured["day"])
                game_date = datetime(year, month, day)
            except Exception as e:
                error = f'Failed to parse date from url_id "{url_id} ({data_set})":\n{repr(e)}'
                raise ValueError(error)
            converted_url_ids.append(game_date)
        return converted_url_ids

    def get_cached_avg_pitch_times(self):
        return {
            "pitch_delta": {
                "total": 3903840.0,
                "count": 158291,
                "avg": 25.0,
                "max": 77.0,
                "min": 1.0,
                "range": 76.0,
            },
            "inning_delta": {
                "total": 1843148.0,
                "count": 11837,
                "avg": 156.0,
                "max": 242.0,
                "min": 92.0,
                "range": 150.0,
            },
        }

    def calculate_avg_pitch_times(self):
        game_ids = PitchAppScrapeStatus.get_bbref_game_ids_all_missing_pfx_is_valid(
            self.db_session
        )
        pitch_delta_samples = []
        inning_delta_samples = []
        for bbref_game_id in game_ids:
            combined_data = self.get_json_combined_data(bbref_game_id)
            if not combined_data:
                continue
            (
                pitch_deltas,
                pitch_delta,
                inning_deltas,
                inning_delta,
            ) = calc_avg_time_between_pitches(combined_data)
            pitch_delta_samples.extend(pitch_deltas)
            inning_delta_samples.extend(inning_deltas)
        pitch_delta_combined = {}
        if len(pitch_delta_samples):
            pitch_delta_samples = trim_data_set(pitch_delta_samples, st_dev_limit=0.5)
            pitch_delta_combined = {
                "total": sum(pitch_delta_samples),
                "count": len(pitch_delta_samples),
                "avg": sum(pitch_delta_samples) / len(pitch_delta_samples),
                "max": max(pitch_delta_samples),
                "min": min(pitch_delta_samples),
                "range": max(pitch_delta_samples) - min(pitch_delta_samples),
            }
        inning_delta_combined = {}
        if len(inning_delta_samples):
            inning_delta_samples = trim_data_set(inning_delta_samples, st_dev_limit=0.5)
            inning_delta_combined = {
                "total": sum(inning_delta_samples),
                "count": len(inning_delta_samples),
                "avg": sum(inning_delta_samples) / len(inning_delta_samples),
                "max": max(inning_delta_samples),
                "min": min(inning_delta_samples),
                "range": max(inning_delta_samples) - min(inning_delta_samples),
            }
        return {"pitch_delta": pitch_delta_combined, "inning_delta": inning_delta_combined}

    def combine_boxscore_and_pfx_data(self, bbref_game_id):
        result = self.get_bbref_boxscore(bbref_game_id)
        if result.failure:
            return result
        boxscore = result.value
        result = self.get_all_pitchfx_logs_for_game(bbref_game_id)
        if result.failure:
            return result
        pitchfx_logs = result.value
        avg_pitch_times = self.get_cached_avg_pitch_times()
        result = self.combine_data.execute(boxscore, pitchfx_logs, avg_pitch_times)
        if result.failure:
            return result
        updated_boxscore_dict = result.value
        return self.save_json_combined_data(updated_boxscore_dict)

    def generate_investigative_materials(self, bbref_game_id):
        result = self.get_bbref_boxscore(bbref_game_id)
        if result.failure:
            return result
        boxscore = result.value
        result = self.get_all_pitchfx_logs_for_game(bbref_game_id)
        if result.failure:
            return result
        pitchfx_logs = result.value
        return self.combine_data.generate_investigative_materials(boxscore, pitchfx_logs)
