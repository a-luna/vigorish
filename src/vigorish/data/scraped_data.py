from datetime import datetime
from dateutil import parser
from pathlib import Path

from vigorish.config.database import (
    DateScrapeStatus,
    GameScrapeStatus,
    PitchAppScrapeStatus,
    Season,
)
from vigorish.data.file_helper import FileHelper
from vigorish.data.html_storage import HtmlStorage
from vigorish.data.json_storage import JsonStorage
from vigorish.data.util import calc_pitch_metrics, process_data_set
from vigorish.data.process.combine_scraped_data import CombineScrapedData
from vigorish.enums import DataSet, DocFormat
from vigorish.status.update_status_combined_data import update_pitch_apps_for_game_combined_data
from vigorish.util.dt_format_strings import DT_AWARE, DATE_ONLY_2
from vigorish.util.numeric_helpers import trim_data_set
from vigorish.util.regex import URL_ID_REGEX, URL_ID_CONVERT_REGEX
from vigorish.util.result import Result
from vigorish.util.string_helpers import validate_bbref_game_id


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
        successful = self.get_all_bbref_game_ids_all_pitchfx_logs_are_valid()
        failed = self.get_all_bbref_game_ids_combined_data_fail()
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

    def get_all_bbref_game_ids_combined_data_fail(self):
        return {
            s.year: s.get_all_bbref_game_ids_combined_data_fail()
            for s in Season.all_regular_seasons(self.db_session)
        }

    def get_all_bbref_game_ids_pitchfx_data_error(self):
        return {
            s.year: s.get_all_bbref_game_ids_pitchfx_data_error()
            for s in Season.all_regular_seasons(self.db_session)
        }

    def get_all_bbref_game_ids_all_pitchfx_logs_are_valid(self):
        return {
            s.year: s.get_all_bbref_game_ids_all_pitchfx_logs_are_valid()
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
            "pitch_metrics": {
                "total": 3903840.0,
                "count": 158291,
                "avg": 25.0,
                "max": 77.0,
                "min": 1.0,
                "range": 76.0,
            },
            "inning_metrics": {
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
        pitch_samples = []
        inning_samples = []
        for bbref_game_id in game_ids:
            combined_data = self.get_json_combined_data(bbref_game_id)
            if not combined_data:
                continue
            (game_pitch_samples, _, game_inning_samples, _,) = calc_pitch_metrics(combined_data)
            pitch_samples.extend(game_pitch_samples)
            inning_samples.extend(game_inning_samples)
        pitch_metrics = process_data_set(pitch_samples, trim=True, st_dev=0.5)
        inning_metrics = process_data_set(inning_samples, trim=True, st_dev=0.5)
        return {"pitch_metrics": pitch_metrics, "inning_metrics": inning_metrics}

    def combine_boxscore_and_pfx_data(self, bbref_game_id):
        game_status = GameScrapeStatus.find_by_bbref_game_id(self.db_session, bbref_game_id)
        result = self.get_bbref_boxscore(bbref_game_id)
        if result.failure:
            return {"gather_scraped_data_success": False, "error": result.error}
        boxscore = result.value
        result = self.get_all_pitchfx_logs_for_game(bbref_game_id)
        if result.failure:
            return {"gather_scraped_data_success": False, "error": result.error}
        pitchfx_logs = result.value
        result = self.combine_data.execute(game_status, boxscore, pitchfx_logs)
        if result.failure:
            setattr(game_status, "combined_data_success", 0)
            setattr(game_status, "combined_data_fail", 1)
            self.db_session.commit()
            return {
                "gather_scraped_data_success": True,
                "combined_data_success": False,
                "error": result.error,
            }
        setattr(game_status, "combined_data_success", 1)
        setattr(game_status, "combined_data_fail", 0)
        self.db_session.commit()
        combined_data = result.value
        result = self.save_json_combined_data(combined_data)
        if result.failure:
            return result
        result = update_pitch_apps_for_game_combined_data(self.db_session, combined_data)
        if result.failure:
            return {
                "gather_scraped_data_success": True,
                "combined_data_success": True,
                "update_pitch_apps_success": False,
                "error": result.error,
            }
        return {
            "gather_scraped_data_success": True,
            "combined_data_success": True,
            "update_pitch_apps_success": True,
            "results": result.value,
        }

    def investigate_errors(self, bbref_game_id):
        game_status = GameScrapeStatus.find_by_bbref_game_id(self.db_session, bbref_game_id)
        result = self.get_bbref_boxscore(bbref_game_id)
        if result.failure:
            return {"gather_scraped_data_success": False, "error": result.error}
        boxscore = result.value
        result = self.get_all_pitchfx_logs_for_game(bbref_game_id)
        if result.failure:
            return {"gather_scraped_data_success": False, "error": result.error}
        pitchfx_logs = result.value
        return self.combine_data.investigate(game_status, boxscore, pitchfx_logs)

    def prune_pitchfx_data_for_display(self, pfx):
        return {
            "pitcher_name": pfx["pitcher_name"],
            "pitch_app_id": pfx["pitch_app_id"],
            "pitcher_id": pfx["pitcher_id"],
            "batter_id": pfx["batter_id"],
            "pitcher_team_id_bb": pfx["pitcher_team_id_bb"],
            "opponent_team_id_bb": pfx["opponent_team_id_bb"],
            "bb_game_id": pfx["bb_game_id"],
            "bbref_game_id": pfx["bbref_game_id"],
            "ab_total": pfx["ab_total"],
            "ab_count": pfx["ab_count"],
            "ab_id": pfx["ab_id"],
            "des": pfx["des"],
            "type": pfx["type"],
            "id": pfx["id"],
            "sz_top": pfx["sz_top"],
            "sz_bot": pfx["sz_bot"],
            "mlbam_pitch_name": pfx["mlbam_pitch_name"],
            "zone_location": pfx["zone_location"],
            "stand": pfx["stand"],
            "strikes": pfx["strikes"],
            "balls": pfx["balls"],
            "p_throws": pfx["p_throws"],
            "pdes": pfx["pdes"],
            "spin": pfx["spin"],
            "inning": pfx["inning"],
            "pfx_x": pfx["pfx_x"],
            "pfx_z": pfx["pfx_z"],
            "x0": pfx["x0"],
            "y0": pfx["y0"],
            "z0": pfx["z0"],
            "start_speed": pfx["start_speed"],
            "px": pfx["px"],
            "pz": pfx["pz"],
            "at_bat_id": pfx["at_bat_id"],
            "has_zone_location": pfx["has_zone_location"],
            "time_pitch_thrown_str": pfx["time_pitch_thrown_str"],
            "seconds_since_game_start": pfx["seconds_since_game_start"],
        }
