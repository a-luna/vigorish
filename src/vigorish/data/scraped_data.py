from datetime import datetime
from pathlib import Path

from sqlalchemy import select

from vigorish.data.file_helper import FileHelper
from vigorish.data.html_storage import HtmlStorage
from vigorish.data.json_storage import JsonStorage
from vigorish.database import (
    DateScrapeStatus,
    GameScrapeStatus,
    PitchApp_PitchType_All_View,
    PitchApp_PitchType_Left_View,
    PitchApp_PitchType_Right_View,
    PitchAppScrapeStatus,
    Season,
)
from vigorish.database import Season_Game_PitchApp_View as Season_View
from vigorish.enums import DataSet
from vigorish.util.pitch_calcs import get_metrics_for_all_pitch_types
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

    def get_s3_folderpath(self, file_type, data_set, year):  # pragma: no cover
        return self.file_helper.s3_folderpath_dict[file_type][data_set].resolve(year=year)

    def check_s3_bucket(self):  # pragma: no cover
        return self.file_helper.check_s3_bucket()

    def create_all_folderpaths(self, year):
        return self.file_helper.create_all_folderpaths(year)

    def save_html(self, data_set, url_id, html):
        return self.html_storage.save_html(data_set, url_id, html)

    def get_html(self, data_set, url_id):
        return self.html_storage.get_html(data_set, url_id)

    def save_json(self, data_set, parsed_data):
        return self.json_storage.save_json(data_set, parsed_data)

    def get_scraped_data(self, data_set, url_id, apply_patch_list=True):
        data = self.json_storage.get_scraped_data(data_set, url_id)
        if not apply_patch_list:
            return data
        result = self.apply_patch_list(data_set, url_id, data)
        return result.value if result.success else None

    def get_brooks_games_for_date(self, game_date, apply_patch_list=True):
        return self.get_scraped_data(DataSet.BROOKS_GAMES_FOR_DATE, game_date, apply_patch_list)

    def get_brooks_pitch_logs_for_game(self, bb_game_id):
        return self.get_scraped_data(DataSet.BROOKS_PITCH_LOGS, bb_game_id, apply_patch_list=False)

    def get_brooks_pitchfx_log(self, pitch_app_id):
        return self.get_scraped_data(DataSet.BROOKS_PITCHFX, pitch_app_id, apply_patch_list=False)

    def get_all_brooks_pitchfx_logs_for_game(self, bbref_game_id, apply_patch_list=True):
        pitchfx_logs = [
            self.get_brooks_pitchfx_log(pitch_app_id)
            for pitch_app_id in self.get_all_pitch_app_ids_with_pfx_data_for_game(bbref_game_id)
        ]
        return (
            Result.Fail(f"Failed to retrieve all pitchfx logs for game {bbref_game_id}")
            if not all(pfx_log for pfx_log in pitchfx_logs)
            else self.apply_patch_list(DataSet.BROOKS_PITCHFX, bbref_game_id, pitchfx_logs)
            if apply_patch_list
            else Result.Ok(pitchfx_logs)
        )

    def get_all_pitch_app_ids_with_pfx_data_for_game(self, bbref_game_id):
        return PitchAppScrapeStatus.get_all_pitch_app_ids_with_pfx_data_for_game(
            self.db_session, bbref_game_id
        )

    def get_bbref_games_for_date(self, game_date, apply_patch_list=True):
        return self.get_scraped_data(DataSet.BBREF_GAMES_FOR_DATE, game_date, apply_patch_list)

    def get_bbref_boxscore(self, bbref_game_id, apply_patch_list=True):
        return self.get_scraped_data(DataSet.BBREF_BOXSCORES, bbref_game_id, apply_patch_list)

    def save_patch_list(self, data_set, patch_list):
        return self.json_storage.save_patch_list(data_set, patch_list)

    def get_patch_list(self, data_set, url_id):
        return self.json_storage.get_patch_list(data_set, url_id)

    def apply_patch_list(self, data_set, url_id, data_to_patch):
        patch_list = self.get_patch_list(data_set, url_id)
        if not patch_list:
            return Result.Ok(data_to_patch)
        kwargs = self.get_data_required_for_patch_list(data_set, url_id)
        return patch_list.apply(data_to_patch, **kwargs)

    def get_data_required_for_patch_list(self, data_set, url_id):
        req_data_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.get_data_for_brooks_games_for_date_patch_list,
            DataSet.BROOKS_PITCHFX: self.get_data_for_brooks_pitchfx_logs_patch_list,
            DataSet.BBREF_GAMES_FOR_DATE: self.get_data_for_bbref_games_for_date_patch_list,
        }
        return req_data_dict[data_set](url_id) if data_set in req_data_dict else {}

    def get_data_for_brooks_games_for_date_patch_list(self, url_id):
        return {"db_session": self.db_session}

    def get_data_for_brooks_pitchfx_logs_patch_list(self, url_id):
        return {"boxscore": self.get_bbref_boxscore(url_id), "db_session": self.db_session}

    def get_data_for_bbref_games_for_date_patch_list(self, url_id):
        return {"db_session": self.db_session}

    def save_combined_game_data(self, combined_data):
        return self.json_storage.save_combined_game_data(combined_data)

    def get_combined_game_data(self, bbref_game_id):
        return self.json_storage.get_combined_game_data(bbref_game_id)

    def get_all_brooks_pitch_logs_for_date(self, game_date):
        brooks_game_ids = GameScrapeStatus.get_all_brooks_game_ids_for_date(
            self.db_session, game_date
        )
        pitch_logs = []
        for game_id in brooks_game_ids:
            pitch_log = self.get_brooks_pitch_logs_for_game(game_id)
            if not pitch_log:
                continue
            pitch_logs.append(pitch_log)
        return pitch_logs

    def get_metrics_for_pitch_app(self, pitch_app_id):
        pitch_mix_data = {}
        pitch_app = PitchAppScrapeStatus.find_by_pitch_app_id(self.db_session, pitch_app_id)
        s = select([PitchApp_PitchType_All_View]).where(
            PitchApp_PitchType_All_View.id == pitch_app.id
        )
        results = self.db_engine.execute(s).fetchall()
        pitch_mix_data["all"] = get_metrics_for_all_pitch_types(results)
        s = select([PitchApp_PitchType_Left_View]).where(
            PitchApp_PitchType_Left_View.id == pitch_app.id
        )
        results = self.db_engine.execute(s).fetchall()
        pitch_mix_data["left"] = get_metrics_for_all_pitch_types(results)
        s = select([PitchApp_PitchType_Right_View]).where(
            PitchApp_PitchType_Right_View.id == pitch_app.id
        )
        results = self.db_engine.execute(s).fetchall()
        pitch_mix_data["right"] = get_metrics_for_all_pitch_types(results)
        return pitch_mix_data

    def get_scraped_ids_from_local_folder(self, file_type, data_set, year):
        folderpath = self.get_local_folderpath(file_type, data_set, year)
        url_ids = [file.stem for file in Path(folderpath).glob("*.*")]
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

    def get_scraped_ids_from_database(self, data_set, season):
        get_scraped_ids_from_db_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.get_all_brooks_scraped_dates_for_season,
            DataSet.BROOKS_PITCH_LOGS: self.get_all_scraped_brooks_game_ids_for_season,
            DataSet.BROOKS_PITCHFX: self.get_all_scraped_pitch_app_ids_for_season,
            DataSet.BBREF_GAMES_FOR_DATE: self.get_all_bbref_scraped_dates_for_season,
            DataSet.BBREF_BOXSCORES: self.get_all_scraped_bbref_game_ids_for_season,
        }
        return get_scraped_ids_from_db_dict[data_set](season)

    def get_all_brooks_scraped_dates_for_season(self, season):
        scraped_dates = DateScrapeStatus.get_all_brooks_scraped_dates_for_season(
            self.db_session, season.id
        )
        return sorted(scraped_dates)

    def get_all_scraped_brooks_game_ids_for_season(self, season):
        scraped_game_ids = GameScrapeStatus.get_all_scraped_brooks_game_ids_for_season(
            self.db_session, season.id
        )
        return sorted(scraped_game_ids)

    def get_all_scraped_pitch_app_ids_for_season(self, season):
        scraped_pitch_app_ids = PitchAppScrapeStatus.get_all_scraped_pitch_app_ids_for_season(
            self.db_session, season.id
        )
        return sorted(scraped_pitch_app_ids)

    def get_all_bbref_scraped_dates_for_season(self, season):
        scraped_dates = DateScrapeStatus.get_all_bbref_scraped_dates_for_season(
            self.db_session, season.id
        )
        return sorted(scraped_dates)

    def get_all_scraped_bbref_game_ids_for_season(self, season):
        scraped_game_ids = GameScrapeStatus.get_all_scraped_bbref_game_ids_for_season(
            self.db_session, season.id
        )
        return sorted(scraped_game_ids)

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
