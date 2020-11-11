from datetime import datetime
from pathlib import Path

from dateutil import parser

from vigorish.config.database import DateScrapeStatus, PitchAppScrapeStatus, Season
from vigorish.config.database import Season_Game_PitchApp_View as Season_View
from vigorish.data.file_helper import FileHelper
from vigorish.data.html_storage import HtmlStorage
from vigorish.data.json_storage import JsonStorage
from vigorish.enums import DataSet, VigFile
from vigorish.util.regex import URL_ID_CONVERT_REGEX, URL_ID_REGEX
from vigorish.util.result import Result


class ScrapedData:
    def __init__(self, db_engine, db_session, config):
        self.db_engine = db_engine
        self.db_session = db_session
        self.config = config
        self.file_helper = FileHelper(config)
        self.html_storage = HtmlStorage(config, self.file_helper)
        self.json_storage = JsonStorage(config, self.file_helper)

    def get_local_folderpath(self, file_type, data_set, year):
        return self.file_helper.local_folderpath_dict[file_type][data_set].resolve(year=year)

    def get_s3_folderpath(self, file_type, data_set, year):
        return self.file_helper.s3_folderpath_dict[file_type][data_set].resolve(year=year)

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

    def delete_html(self, data_set, url_id):
        return self.html_storage.delete_html(data_set, url_id)

    def save_json(self, data_set, parsed_data):
        return self.json_storage.save_json(data_set, parsed_data)

    def get_scraped_data(self, data_set, url_id):
        return self.json_storage.get_scraped_data(data_set, url_id)

    def save_patch_list(self, data_set, patch_list):
        return self.json_storage.save_patch_list(data_set, patch_list)

    def get_patch_list(self, data_set, url_id):
        return self.json_storage.get_patch_list(data_set, url_id)

    def apply_patch_list(self, data_set, url_id, scraped_data, boxscore=None):
        patch_list = self.get_patch_list(data_set, url_id)
        if not patch_list:
            return Result.Ok(scraped_data)
        return patch_list.apply(scraped_data, self.db_session, boxscore)

    def get_brooks_games_for_date(self, game_date):
        return self.json_storage.get_scraped_data(DataSet.BROOKS_GAMES_FOR_DATE, game_date)

    def get_brooks_pitch_logs_for_game(self, bb_game_id):
        return self.json_storage.get_scraped_data(DataSet.BROOKS_PITCH_LOGS, bb_game_id)

    def get_brooks_pitchfx_log(self, pitch_app_id):
        return self.json_storage.get_scraped_data(DataSet.BROOKS_PITCHFX, pitch_app_id)

    def get_bbref_games_for_date(self, game_date):
        return self.json_storage.get_scraped_data(DataSet.BBREF_GAMES_FOR_DATE, game_date)

    def get_bbref_boxscore(self, bbref_game_id):
        return self.json_storage.get_scraped_data(DataSet.BBREF_BOXSCORES, bbref_game_id)

    def save_combined_game_data(self, combined_data):
        return self.json_storage.save_combined_game_data(combined_data)

    def get_combined_game_data(self, bbref_game_id):
        return self.json_storage.get_combined_game_data(bbref_game_id)

    def get_all_brooks_pitch_logs_for_date(self, game_date):
        brooks_game_ids = DateScrapeStatus.get_all_brooks_game_ids_for_date(
            self.db_session, game_date
        )
        pitch_logs = []
        for game_id in brooks_game_ids:
            pitch_log = self.get_brooks_pitch_logs_for_game(game_id)
            if not pitch_log:
                continue
            pitch_logs.append(pitch_log)
        return pitch_logs

    def get_all_pitchfx_logs_for_game(self, bbref_game_id):
        p_app_ids = PitchAppScrapeStatus.get_all_scraped_pitch_app_ids_for_game_with_pitchfx_data(
            self.db_session, bbref_game_id
        )
        pitchfx_logs = [self.get_brooks_pitchfx_log(pitch_app_id) for pitch_app_id in p_app_ids]
        if any(not pfx_log for pfx_log in pitchfx_logs):
            return Result.Fail(f"Failed to retrieve all pitchfx logs for game {bbref_game_id}")
        return Result.Ok(pitchfx_logs)

    def get_all_brooks_dates_scraped_html(self, year):
        html_folder = self.file_helper.get_s3_folderpath(
            file_type=VigFile.SCRAPED_HTML, data_set=DataSet.BROOKS_GAMES_FOR_DATE, year=year
        )
        scraped_dates = [
            Path(obj.key).stem
            for obj in self.file_helper.get_all_object_keys_in_s3_bucket()
            if html_folder in obj.key
        ]
        return scraped_dates

    def get_all_brooks_dates_scraped(self, year):
        json_folder = self.file_helper.get_s3_folderpath(
            file_type=VigFile.PARSED_JSON, data_set=DataSet.BROOKS_GAMES_FOR_DATE, year=year
        )
        html_folder = self.file_helper.get_s3_folderpath(
            file_type=VigFile.SCRAPED_HTML, data_set=DataSet.BROOKS_GAMES_FOR_DATE, year=year
        )
        scraped_dates = [
            parser.parse(Path(obj.key).stem[-10:])
            for obj in self.file_helper.get_all_object_keys_in_s3_bucket()
            if json_folder in obj.key and html_folder not in obj.key
        ]
        return Result.Ok(scraped_dates)

    def get_all_brooks_pitch_logs_scraped_html(self, year):
        html_folder = self.file_helper.get_s3_folderpath(
            file_type=VigFile.SCRAPED_HTML, data_set=DataSet.BROOKS_PITCH_LOGS, year=year
        )
        scraped_gameids = [
            Path(obj.key).stem
            for obj in self.file_helper.get_all_object_keys_in_s3_bucket()
            if html_folder in obj.key
        ]
        return scraped_gameids

    def get_all_scraped_brooks_game_ids(self, year):
        json_folder = self.file_helper.get_s3_folderpath(
            file_type=VigFile.PARSED_JSON, data_set=DataSet.BROOKS_PITCH_LOGS, year=year
        )
        html_folder = self.file_helper.get_s3_folderpath(
            file_type=VigFile.SCRAPED_HTML, data_set=DataSet.BROOKS_PITCH_LOGS, year=year
        )
        scraped_gameids = [
            Path(obj.key).stem
            for obj in self.file_helper.get_all_object_keys_in_s3_bucket()
            if json_folder in obj.key and html_folder not in obj.key
        ]
        return Result.Ok(scraped_gameids)

    def get_all_pitchfx_pitch_app_ids_scraped_html(self, year):
        html_folder = self.file_helper.get_s3_folderpath(
            file_type=VigFile.SCRAPED_HTML, data_set=DataSet.BROOKS_PITCHFX, year=year
        )
        scraped_pitch_app_ids = [
            Path(obj.key).stem
            for obj in self.file_helper.get_all_object_keys_in_s3_bucket()
            if html_folder in obj.key
        ]
        return scraped_pitch_app_ids

    def get_all_scraped_pitchfx_pitch_app_ids(self, year):
        json_folder = self.file_helper.get_s3_folderpath(
            file_type=VigFile.PARSED_JSON, data_set=DataSet.BROOKS_PITCHFX, year=year
        )
        html_folder = self.file_helper.get_s3_folderpath(
            file_type=VigFile.SCRAPED_HTML, data_set=DataSet.BROOKS_PITCHFX, year=year
        )
        scraped_pitch_app_ids = [
            Path(obj.key).stem
            for obj in self.file_helper.get_all_object_keys_in_s3_bucket()
            if json_folder in obj.key and html_folder not in obj.key
        ]
        return scraped_pitch_app_ids

    def get_all_bbref_dates_scraped_html(self, year):
        html_folder = self.file_helper.get_s3_folderpath(
            file_type=VigFile.SCRAPED_HTML, data_set=DataSet.BBREF_GAMES_FOR_DATE, year=year
        )
        scraped_dates = [
            Path(obj.key).stem
            for obj in self.file_helper.get_all_object_keys_in_s3_bucket()
            if html_folder in obj.key
        ]
        return scraped_dates

    def get_all_bbref_dates_scraped(self, year):
        json_folder = self.file_helper.get_s3_folderpath(
            file_type=VigFile.PARSED_JSON, data_set=DataSet.BBREF_GAMES_FOR_DATE, year=year
        )
        html_folder = self.file_helper.get_s3_folderpath(
            file_type=VigFile.SCRAPED_HTML, data_set=DataSet.BBREF_GAMES_FOR_DATE, year=year
        )
        scraped_dates = [
            parser.parse(Path(obj.key).stem[-10:])
            for obj in self.file_helper.get_all_object_keys_in_s3_bucket()
            if json_folder in obj.key and html_folder not in obj.key
        ]
        return Result.Ok(scraped_dates)

    def get_all_bbref_game_ids_scraped_html(self, year):
        html_folder = self.file_helper.get_s3_folderpath(
            file_type=VigFile.SCRAPED_HTML, data_set=DataSet.BBREF_BOXSCORES, year=year
        )
        scraped_game_ids = [
            Path(obj.key).stem
            for obj in self.file_helper.get_all_object_keys_in_s3_bucket()
            if html_folder in obj.key
        ]
        return scraped_game_ids

    def get_all_scraped_bbref_game_ids(self, year):
        json_folder = self.file_helper.get_s3_folderpath(
            file_type=VigFile.PARSED_JSON, data_set=DataSet.BBREF_BOXSCORES, year=year
        )
        html_folder = self.file_helper.get_s3_folderpath(
            file_type=VigFile.SCRAPED_HTML, data_set=DataSet.BBREF_BOXSCORES, year=year
        )
        scraped_game_ids = [
            Path(obj.key).stem
            for obj in self.file_helper.get_all_object_keys_in_s3_bucket()
            if json_folder in obj.key and html_folder not in obj.key
        ]
        return Result.Ok(scraped_game_ids)

    def get_audit_report(self):
        all_seasons = Season.all_regular_seasons(self.db_session)
        total_games = self.get_total_games_for_all_seasons(all_seasons)
        scraped = self.get_all_bbref_game_ids_eligible_for_audit(all_seasons)
        successful = self.get_all_bbref_game_ids_all_pitchfx_logs_are_valid(all_seasons)
        failed = self.get_all_bbref_game_ids_combined_data_fail(all_seasons)
        pfx_error = self.get_all_bbref_game_ids_pitchfx_error(all_seasons)
        invalid_pfx = self.get_all_bbref_game_ids_invalid_pitchfx(all_seasons)
        mlb_seasons = list(
            set(
                list(scraped.keys())
                + list(successful.keys())
                + list(failed.keys())
                + list(pfx_error.keys())
                + list(invalid_pfx.keys())
            )
        )
        return {
            year: {
                "total_games": total_games[year],
                "scraped": scraped[year],
                "successful": successful[year],
                "failed": failed[year],
                "pfx_error": pfx_error[year],
                "invalid_pfx": invalid_pfx[year],
            }
            for year in mlb_seasons
            if scraped[year]
            or successful[year]
            or failed[year]
            or pfx_error[year]
            or invalid_pfx[year]
        }

    def get_total_games_for_all_seasons(self, all_seasons):
        return {s.year: s.total_games for s in all_seasons}

    def get_all_bbref_game_ids_eligible_for_audit(self, all_seasons):
        return {
            s.year: Season_View.get_all_bbref_game_ids_eligible_for_audit(self.db_engine, s.year)
            for s in all_seasons
        }

    def get_all_bbref_game_ids_combined_data_fail(self, all_seasons):
        return {s.year: s.get_all_bbref_game_ids_combined_data_fail() for s in all_seasons}

    def get_all_bbref_game_ids_pitchfx_error(self, all_seasons):
        return {
            s.year: Season_View.get_all_bbref_game_ids_pitchfx_error(self.db_engine, s.year)
            for s in all_seasons
        }

    def get_all_bbref_game_ids_invalid_pitchfx(self, all_seasons):
        return {
            s.year: Season_View.get_all_bbref_game_ids_invalid_pitchfx(self.db_engine, s.year)
            for s in all_seasons
        }

    def get_all_bbref_game_ids_all_pitchfx_logs_are_valid(self, all_seasons):
        return {
            s.year: Season_View.get_all_bbref_game_ids_all_pitchfx_logs_are_valid(
                self.db_engine, s.year
            )
            for s in all_seasons
        }

    def get_scraped_ids_from_local_folder(self, file_type, data_set, year):
        folderpath = self.get_local_folderpath(file_type, data_set, year)
        url_ids = [file.stem for file in Path(folderpath).glob("*.*")]
        return self.validate_url_ids(file_type, data_set, url_ids)

    def get_scraped_ids_from_database(self, data_set, year):
        get_scraped_ids_from_db_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.get_all_brooks_dates_scraped,
            DataSet.BROOKS_PITCH_LOGS: self.get_all_scraped_brooks_game_ids,
            DataSet.BROOKS_PITCHFX: self.get_all_scraped_pitchfx_pitch_app_ids,
            DataSet.BBREF_GAMES_FOR_DATE: self.get_all_bbref_dates_scraped,
            DataSet.BBREF_BOXSCORES: self.get_all_scraped_bbref_game_ids,
        }
        return get_scraped_ids_from_db_dict[data_set](year)

    def get_scraped_ids_from_s3(self, file_type, data_set, year):
        scraped_html_s3_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.get_all_brooks_dates_scraped_html,
            DataSet.BROOKS_PITCH_LOGS: self.get_all_brooks_pitch_logs_scraped_html,
            DataSet.BROOKS_PITCHFX: self.get_all_pitchfx_pitch_app_ids_scraped_html,
            DataSet.BBREF_GAMES_FOR_DATE: self.get_all_bbref_dates_scraped_html,
            DataSet.BBREF_BOXSCORES: self.get_all_bbref_game_ids_scraped_html,
        }
        url_ids = scraped_html_s3_dict[data_set](year)
        return self.validate_url_ids(file_type, data_set, url_ids)

    def validate_url_ids(self, file_type, data_set, url_ids):
        url_ids = [uid for uid in url_ids if URL_ID_REGEX[file_type][data_set].search(uid)]
        url_ids = self.convert_url_ids(file_type, data_set, url_ids)
        return sorted(url_ids)

    def convert_url_ids(self, file_type, data_set, url_ids):
        if data_set not in URL_ID_CONVERT_REGEX[file_type]:
            return url_ids
        convert_regex = URL_ID_CONVERT_REGEX[file_type][data_set]
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
