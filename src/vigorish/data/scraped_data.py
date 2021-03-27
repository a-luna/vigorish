from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Union

import vigorish.database as db
from vigorish.data.file_helper import FileHelper
from vigorish.data.html_storage import HtmlStorage
from vigorish.data.json_storage import JsonStorage
from vigorish.data.metrics import (
    BatStatsMetrics,
    PitchFxMetrics,
    PitchFxMetricsCollection,
    PitchStatsMetrics,
)
from vigorish.data.name_search import PlayerNameSearch
from vigorish.enums import DataSet, DefensePosition, PitchType, VigFile
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
        self.name_search = PlayerNameSearch(db_session)

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

    def get_brooks_pitchfx_log(self, pitch_app_id, apply_patch_list=False):
        return self.get_scraped_data(DataSet.BROOKS_PITCHFX, pitch_app_id, apply_patch_list)

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
        return db.PitchAppScrapeStatus.get_all_pitch_app_ids_with_pfx_data_for_game(self.db_session, bbref_game_id)

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
        brooks_game_ids = db.GameScrapeStatus.get_all_brooks_game_ids_for_date(self.db_session, game_date)
        pitch_logs = []
        for game_id in brooks_game_ids:
            pitch_log = self.get_brooks_pitch_logs_for_game(game_id)
            if not pitch_log:
                continue
            pitch_logs.append(pitch_log)
        return pitch_logs

    def get_scraped_ids_from_local_folder(self, file_type, data_set, year):
        folderpath = self.get_local_folderpath(file_type, data_set, year)
        url_ids = [file.stem for file in Path(folderpath).glob("*.*")]
        return self.validate_url_ids(file_type, data_set, url_ids)

    def validate_url_ids(self, file_type, data_set, url_ids):
        url_ids = filter(lambda x: URL_ID_REGEX[file_type][data_set].search(x), url_ids)
        url_ids = self.convert_url_ids(file_type, data_set, url_ids)
        return sorted(url_ids)

    def convert_url_ids(self, file_type, data_set, url_ids):
        if data_set not in URL_ID_CONVERT_REGEX[file_type]:
            return list(url_ids)
        convert_regex = URL_ID_CONVERT_REGEX[file_type][data_set]
        converted_url_ids = []
        for url_id in url_ids:
            match = convert_regex.search(url_id)
            if not match:
                raise ValueError(f"URL identifier is invalid: {url_id} ({data_set})")
            if file_type == VigFile.COMBINED_GAME_DATA:
                converted = match.groupdict()["bbref_game_id"]
            else:
                converted = self.convert_url_id_to_date(data_set, url_id, match.groupdict())
            converted_url_ids.append(converted)
        return converted_url_ids

    def convert_url_id_to_date(self, data_set, url_id, group_dict):
        try:
            year = int(group_dict["year"])
            month = int(group_dict["month"])
            day = int(group_dict["day"])
            return datetime(year, month, day)
        except Exception as e:
            error = f'Failed to parse date from url_id "{url_id} ({data_set})":\n{repr(e)}'
            raise ValueError(error)

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
        scraped_dates = db.DateScrapeStatus.get_all_brooks_scraped_dates_for_season(self.db_session, season.id)
        return sorted(scraped_dates)

    def get_all_scraped_brooks_game_ids_for_season(self, season):
        scraped_game_ids = db.GameScrapeStatus.get_all_scraped_brooks_game_ids_for_season(self.db_session, season.id)
        return sorted(scraped_game_ids)

    def get_all_scraped_pitch_app_ids_for_season(self, season):
        scraped_pitch_app_ids = db.PitchAppScrapeStatus.get_all_scraped_pitch_app_ids_for_season(
            self.db_session, season.id
        )
        return sorted(scraped_pitch_app_ids)

    def get_all_bbref_scraped_dates_for_season(self, season):
        scraped_dates = db.DateScrapeStatus.get_all_bbref_scraped_dates_for_season(self.db_session, season.id)
        return sorted(scraped_dates)

    def get_all_scraped_bbref_game_ids_for_season(self, season):
        scraped_game_ids = db.GameScrapeStatus.get_all_scraped_bbref_game_ids_for_season(self.db_session, season.id)
        return sorted(scraped_game_ids)

    def get_audit_report(self):
        all_seasons = db.Season.get_all_regular_seasons(self.db_session)
        total_games = db.Season.get_total_games_for_all_seasons(self.db_session)
        scraped = self.get_all_bbref_game_ids_eligible_for_audit(all_seasons)
        successful = self.get_all_bbref_game_ids_all_pitchfx_logs_are_valid(all_seasons)
        failed = self.get_all_bbref_game_ids_combined_data_fail(all_seasons)
        pfx_error = self.get_all_bbref_game_ids_pitchfx_error(all_seasons)
        invalid_pfx = self.get_all_bbref_game_ids_invalid_pitchfx(all_seasons)
        return {
            s.year: {
                "total_games": total_games[s.year],
                "scraped": scraped[s.year],
                "successful": successful[s.year],
                "failed": failed[s.year],
                "pfx_error": pfx_error[s.year],
                "invalid_pfx": invalid_pfx[s.year],
            }
            for s in all_seasons
            if scraped[s.year] or successful[s.year] or failed[s.year] or pfx_error[s.year] or invalid_pfx[s.year]
        }

    def get_all_bbref_game_ids_eligible_for_audit(self, all_seasons):
        return {
            s.year: db.Season_Game_PitchApp_View.get_all_bbref_game_ids_eligible_for_audit(self.db_engine, s.year)
            for s in all_seasons
        }

    def get_all_bbref_game_ids_combined_data_fail(self, all_seasons):
        return {s.year: s.get_all_bbref_game_ids_combined_data_fail() for s in all_seasons}

    def get_all_bbref_game_ids_pitchfx_error(self, all_seasons):
        return {
            s.year: db.Season_Game_PitchApp_View.get_all_bbref_game_ids_pitchfx_error(self.db_engine, s.year)
            for s in all_seasons
        }

    def get_all_bbref_game_ids_invalid_pitchfx(self, all_seasons):
        return {
            s.year: db.Season_Game_PitchApp_View.get_all_bbref_game_ids_invalid_pitchfx(self.db_engine, s.year)
            for s in all_seasons
        }

    def get_all_bbref_game_ids_all_pitchfx_logs_are_valid(self, all_seasons):
        return {
            s.year: db.Season_Game_PitchApp_View.get_all_bbref_game_ids_all_pitchfx_logs_are_valid(
                self.db_engine, s.year
            )
            for s in all_seasons
        }

    def get_all_seasons_with_data_for_player(self, player_mlb_id: int) -> List[db.Season]:
        seasons_with_pitch_data = db.PitchStats.get_all_seasons_with_data_for_player(self.db_session, player_mlb_id)
        seasons_with_bat_data = db.BatStats.get_all_seasons_with_data_for_player(self.db_session, player_mlb_id)
        seasons_with_player_data = list(set(list(seasons_with_pitch_data) + list(seasons_with_bat_data)))
        return sorted(seasons_with_player_data, key=lambda x: x.year) if seasons_with_player_data else []

    def player_name_search(self, query):
        return self.name_search.fuzzy_match(query)

    # TEAM PITCH STATS

    def get_pitch_stats_for_team(self, team_id_bbref: str, year: int) -> PitchStatsMetrics:
        return db.Team_PitchStats_By_Year_View.get_pitch_stats_for_team(self.db_engine, team_id_bbref, year)

    def get_pitch_stats_for_sp_for_team(self, team_id_bbref: str, year: int) -> PitchStatsMetrics:
        return db.Team_PitchStats_SP_By_Year_View.get_pitch_stats_for_sp_for_team(self.db_engine, team_id_bbref, year)

    def get_pitch_stats_for_rp_for_team(self, team_id_bbref: str, year: int) -> PitchStatsMetrics:
        return db.Team_PitchStats_RP_By_Year_View.get_pitch_stats_for_rp_for_team(self.db_engine, team_id_bbref, year)

    def get_pitch_stats_by_year_for_team(self, team_id_bbref: str) -> Dict[int, PitchStatsMetrics]:
        team_stats = db.Team_PitchStats_By_Year_View.get_pitch_stats_by_year_for_team(self.db_engine, team_id_bbref)
        return {s.year: s for s in team_stats}

    def get_pitch_stats_for_sp_by_year_for_team(self, team_id_bbref: str) -> Dict[int, PitchStatsMetrics]:
        team_stats = db.Team_PitchStats_SP_By_Year_View.get_pitch_stats_for_sp_by_year_for_team(
            self.db_engine, team_id_bbref
        )
        return {s.year: s for s in team_stats}

    def get_pitch_stats_for_rp_by_year_for_team(self, team_id_bbref: str) -> Dict[int, PitchStatsMetrics]:
        team_stats = db.Team_PitchStats_RP_By_Year_View.get_pitch_stats_for_rp_by_year_for_team(
            self.db_engine, team_id_bbref
        )
        return {s.year: s for s in team_stats}

    def get_pitch_stats_by_player_for_team(self, team_id_bbref: str, year: int) -> List[PitchStatsMetrics]:
        pitch_stats = db.Team_PitchStats_By_Player_By_Year_View.get_pitch_stats_by_player_for_team(
            self.db_engine, team_id_bbref, year
        )
        return sorted(pitch_stats, key=lambda x: x.re24_pitch, reverse=True)

    def get_pitch_stats_for_sp_by_player_for_team(self, team_id_bbref: str, year: int) -> List[PitchStatsMetrics]:
        pitch_stats = db.Team_PitchStats_SP_By_Player_By_Year_View.get_pitch_stats_for_sp_by_player_for_team(
            self.db_engine, team_id_bbref, year
        )
        return sorted(pitch_stats, key=lambda x: x.re24_pitch, reverse=True)

    def get_pitch_stats_for_rp_by_player_for_team(self, team_id_bbref: str, year: int) -> List[PitchStatsMetrics]:
        pitch_stats = db.Team_PitchStats_RP_By_Player_By_Year_View.get_pitch_stats_for_rp_by_player_for_team(
            self.db_engine, team_id_bbref, year
        )
        return sorted(pitch_stats, key=lambda x: x.re24_pitch, reverse=True)

    def get_pitch_stats_for_season_for_all_teams(self, year: int) -> Dict[str, PitchStatsMetrics]:
        team_stats = db.Team_PitchStats_By_Year_View.get_pitch_stats_for_season_for_all_teams(self.db_engine, year)
        return _sort_and_map_team_pitch_stats(team_stats)

    def get_pitch_stats_for_sp_for_season_for_all_teams(self, year: int) -> Dict[str, PitchStatsMetrics]:
        taem_stats = db.Team_PitchStats_SP_By_Year_View.get_pitch_stats_for_sp_for_season_for_all_teams(
            self.db_engine, year
        )
        return _sort_and_map_team_pitch_stats(taem_stats)

    def get_pitch_stats_for_rp_for_season_for_all_teams(self, year: int) -> Dict[str, PitchStatsMetrics]:
        team_stats = db.Team_PitchStats_RP_By_Year_View.get_pitch_stats_for_rp_for_season_for_all_teams(
            self.db_engine, year
        )
        return _sort_and_map_team_pitch_stats(team_stats)

    # TEAM BAT STATS

    def get_bat_stats_for_team(self, team_id_bbref: str, year: int) -> BatStatsMetrics:
        return db.Team_BatStats_By_Year_View.get_bat_stats_for_team(self.db_engine, team_id_bbref, year)

    def get_bat_stats_by_lineup_spot_for_team(self, team_id_bbref: str, year: int) -> List[BatStatsMetrics]:
        return db.Team_BatStats_By_BatOrder_By_Year.get_bat_stats_by_lineup_spot_for_team(
            self.db_engine, team_id_bbref, year
        )

    def get_bat_stats_by_defpos_for_team(self, team_id_bbref: str, year: int) -> List[BatStatsMetrics]:
        return db.Team_BatStats_By_DefPosition_By_Year.get_bat_stats_by_defpos_for_team(
            self.db_engine, team_id_bbref, year
        )

    def get_bat_stats_for_starters_for_team(self, team_id_bbref: str, year: int) -> BatStatsMetrics:
        return db.Team_BatStats_For_Starters_By_Year.get_bat_stats_for_starters_for_team(
            self.db_engine, team_id_bbref, year
        )

    def get_bat_stats_for_subs_for_team(self, team_id_bbref: str, year: int) -> BatStatsMetrics:
        return db.Team_BatStats_For_Subs_By_Year.get_bat_stats_for_subs_for_team(self.db_engine, team_id_bbref, year)

    def get_bat_stats_by_year_for_team(self, team_id_bbref: str) -> Dict[int, BatStatsMetrics]:
        team_stats = db.Team_BatStats_By_Year_View.get_bat_stats_by_year_for_team(self.db_engine, team_id_bbref)
        return {s.year: s for s in team_stats}

    def get_bat_stats_for_lineup_spot_by_year_for_team(
        self, lineup_spot: int, team_id_bbref: str
    ) -> Dict[int, BatStatsMetrics]:
        team_stats = db.Team_BatStats_By_BatOrder_By_Year.get_bat_stats_for_lineup_spot_by_year_for_team(
            self.db_engine, lineup_spot, team_id_bbref
        )
        return {s.year: s for s in team_stats}

    def get_bat_stats_for_defpos_by_year_for_team(
        self, def_position: DefensePosition, team_id_bbref: str
    ) -> Dict[int, BatStatsMetrics]:
        team_stats = db.Team_BatStats_By_DefPosition_By_Year.get_bat_stats_for_defpos_by_year_for_team(
            self.db_engine, int(def_position), team_id_bbref
        )
        return {s.year: s for s in team_stats}

    def get_bat_stats_for_starters_by_year_for_team(self, team_id_bbref: str) -> Dict[int, BatStatsMetrics]:
        team_stats = db.Team_BatStats_For_Starters_By_Year.get_bat_stats_for_starters_by_year_for_team(
            self.db_engine, team_id_bbref
        )
        return {s.year: s for s in team_stats}

    def get_bat_stats_for_subs_by_year_for_team(self, team_id_bbref: str) -> Dict[int, BatStatsMetrics]:
        team_stats = db.Team_BatStats_For_Subs_By_Year.get_bat_stats_for_subs_by_year_for_team(
            self.db_engine, team_id_bbref
        )
        return {s.year: s for s in team_stats}

    def get_bat_stats_by_player_for_team(self, team_id_bbref: str, year: int) -> List[BatStatsMetrics]:
        team_stats = db.Team_BatStats_By_Player_By_Year_View.get_bat_stats_by_player_for_team(
            self.db_engine, team_id_bbref, year
        )
        return sorted(team_stats, key=lambda x: x.plate_appearances, reverse=True)

    def get_bat_stats_for_lineup_spot_by_player_for_team(
        self, lineup_spot: int, team_id_bbref: str, year: int
    ) -> List[BatStatsMetrics]:
        team_stats = db.Team_BatStats_By_BatOrder_By_Player_By_Year.get_bat_stats_for_lineup_spot_by_player_for_team(
            self.db_engine, lineup_spot, team_id_bbref, year
        )
        return sorted(team_stats, key=lambda x: x.plate_appearances, reverse=True)

    def get_bat_stats_for_defpos_by_player_for_team(
        self, def_position: DefensePosition, team_id_bbref: str, year: int
    ) -> List[BatStatsMetrics]:
        team_stats = db.Team_BatStats_By_DefPosition_By_Player_By_Year.get_bat_stats_for_defpos_by_player_for_team(
            self.db_engine, int(def_position), team_id_bbref, year
        )
        return sorted(team_stats, key=lambda x: x.plate_appearances, reverse=True)

    def get_bat_stats_for_starters_by_player_for_team(self, team_id_bbref: str, year: int) -> List[BatStatsMetrics]:
        team_stats = db.Team_BatStats_For_Starters_By_Player_By_Year.get_bat_stats_for_starters_by_player_for_team(
            self.db_engine, team_id_bbref, year
        )
        return sorted(team_stats, key=lambda x: x.plate_appearances, reverse=True)

    def get_bat_stats_for_subs_by_player_for_team(self, team_id_bbref: str, year: int) -> List[BatStatsMetrics]:
        team_stats = db.Team_BatStats_For_Subs_By_Player_By_Year.get_bat_stats_for_subs_by_player_for_team(
            self.db_engine, team_id_bbref, year
        )
        return sorted(team_stats, key=lambda x: x.plate_appearances, reverse=True)

    def get_bat_stats_for_season_for_all_teams(self, year: int) -> Dict[str, BatStatsMetrics]:
        team_stats = db.Team_BatStats_By_Year_View.get_bat_stats_for_season_for_all_teams(self.db_engine, year)
        return _sort_and_map_team_bat_stats(team_stats)

    def get_bat_stats_for_lineup_spot_for_season_for_all_teams(
        self, lineup_spot: int, year: int
    ) -> Dict[str, BatStatsMetrics]:
        team_batstats = db.Team_BatStats_By_BatOrder_By_Year.get_bat_stats_for_lineup_spot_for_season_for_all_teams(
            self.db_engine, lineup_spot, year
        )
        return _sort_and_map_team_bat_stats(team_batstats)

    def get_bat_stats_for_defpos_for_season_for_all_teams(
        self, def_position: DefensePosition, year: int
    ) -> Dict[str, BatStatsMetrics]:
        team_batstats = db.Team_BatStats_By_DefPosition_By_Year.get_bat_stats_for_defpos_for_season_for_all_teams(
            self.db_engine, int(def_position), year
        )
        return _sort_and_map_team_bat_stats(team_batstats)

    def get_bat_stats_for_starters_for_season_for_all_teams(self, year: int) -> Dict[str, BatStatsMetrics]:
        team_batstats = db.Team_BatStats_For_Starters_By_Year.get_bat_stats_for_starters_for_season_for_all_teams(
            self.db_engine, year
        )
        return _sort_and_map_team_bat_stats(team_batstats)

    def get_bat_stats_for_subs_for_season_for_all_teams(self, year: int) -> Dict[str, BatStatsMetrics]:
        team_batstats = db.Team_BatStats_For_Subs_By_Year.get_bat_stats_for_subs_for_season_for_all_teams(
            self.db_engine, year
        )
        return _sort_and_map_team_bat_stats(team_batstats)

    # PLAYER PITCH STATS

    def get_pitch_stats_for_career_for_player(self, mlb_id: int) -> PitchStatsMetrics:
        return db.PitchStats_All_View.get_pitch_stats_for_career_for_player(self.db_engine, mlb_id)

    def get_pitch_stats_as_sp_for_player(self, mlb_id: int) -> PitchStatsMetrics:
        return db.PitchStats_SP_View.get_pitch_stats_as_sp_for_player(self.db_engine, mlb_id)

    def get_pitch_stats_as_rp_for_player(self, mlb_id: int) -> PitchStatsMetrics:
        return db.PitchStats_RP_View.get_pitch_stats_as_rp_for_player(self.db_engine, mlb_id)

    def get_pitch_stats_by_year_for_player(self, mlb_id: int) -> List[PitchStatsMetrics]:
        return db.PitchStats_By_Year_View.get_pitch_stats_by_year_for_player(self.db_engine, mlb_id)

    def get_pitch_stats_by_team_for_player(self, mlb_id: int) -> List[PitchStatsMetrics]:
        return db.PitchStats_By_Team_View.get_pitch_stats_by_team_for_player(self.db_engine, mlb_id)

    def get_pitch_stats_by_team_by_year_for_player(self, mlb_id: int) -> List[PitchStatsMetrics]:
        return db.PitchStats_By_Team_By_Year_View.get_pitch_stats_by_team_by_year_for_player(self.db_engine, mlb_id)

    def get_pitch_stats_by_opp_team_for_player(self, mlb_id: int) -> List[PitchStatsMetrics]:
        return db.PitchStats_By_Opp_Team_View.get_pitch_stats_by_opp_team_for_player(self.db_engine, mlb_id)

    def get_pitch_stats_by_opp_team_by_year_for_player(self, mlb_id: int) -> List[PitchStatsMetrics]:
        return db.PitchStats_By_Opp_Team_By_Year_View.get_pitch_stats_by_opp_team_by_year_for_player(
            self.db_engine, mlb_id
        )

    # PITCHER PITCHFX STATS

    def get_career_pfx_metrics_for_pitcher(self, mlb_id: int) -> PitchFxMetricsCollection:
        return PitchFxMetricsCollection.from_query_results(
            db.Pitcher_PitchFx_All_View.get_pfx_metrics_for_career_for_pitcher(self.db_engine, mlb_id),
            db.Pitcher_PitchType_All_View.get_pfx_metrics_for_career_for_pitcher(self.db_engine, mlb_id),
        )

    def calculate_pitch_type_percentiles(
        self, p_throws: str, pfx_metrics: PitchFxMetrics
    ) -> Dict[str, Union[PitchType, Tuple[float, float]]]:
        return db.PitchTypePercentile.calculate_pitch_type_percentiles(self.db_session, p_throws, pfx_metrics)

    def get_career_pfx_metrics_vs_rhb_for_pitcher(self, mlb_id: int) -> PitchFxMetricsCollection:
        return PitchFxMetricsCollection.from_query_results(
            db.Pitcher_PitchFx_vs_RHB_View.get_pfx_metrics_vs_rhb_for_pitcher(self.db_engine, mlb_id),
            db.Pitcher_PitchType_vs_RHB_View.get_pfx_metrics_vs_rhb_for_pitcher(self.db_engine, mlb_id),
        )

    def get_career_pfx_metrics_vs_lhb_for_pitcher(self, mlb_id: int) -> PitchFxMetricsCollection:
        return PitchFxMetricsCollection.from_query_results(
            db.Pitcher_PitchFx_vs_LHB_View.get_pfx_metrics_vs_lhb_for_pitcher(self.db_engine, mlb_id),
            db.Pitcher_PitchType_vs_LHB_View.get_pfx_metrics_vs_lhb_for_pitcher(self.db_engine, mlb_id),
        )

    def get_pfx_metrics_for_year_for_pitcher(self, mlb_id: int, year: int) -> PitchFxMetricsCollection:
        return PitchFxMetricsCollection.from_query_results(
            db.Pitcher_PitchFx_All_By_Year_View.get_pfx_metrics_by_year_for_pitcher(self.db_engine, mlb_id, year),
            db.Pitcher_PitchType_All_By_Year_View.get_pfx_metrics_by_year_for_pitcher(self.db_engine, mlb_id, year),
        )

    def get_pfx_metrics_for_year_vs_rhb_for_pitcher(self, mlb_id: int, year: int) -> PitchFxMetricsCollection:
        return PitchFxMetricsCollection.from_query_results(
            db.Pitcher_PitchFx_vs_RHB_By_Year_View.get_pfx_metrics_by_year_for_pitcher(self.db_engine, mlb_id, year),
            db.Pitcher_PitchType_vs_RHB_By_Year_View.get_pfx_metrics_by_year_for_pitcher(self.db_engine, mlb_id, year),
        )

    def get_pfx_metrics_for_year_vs_lhb_for_pitcher(self, mlb_id: int, year: int) -> PitchFxMetricsCollection:
        return PitchFxMetricsCollection.from_query_results(
            db.Pitcher_PitchFx_vs_LHB_By_Year_View.get_pfx_metrics_by_year_for_pitcher(self.db_engine, mlb_id, year),
            db.Pitcher_PitchType_vs_LHB_By_Year_View.get_pfx_metrics_by_year_for_pitcher(self.db_engine, mlb_id, year),
        )

    def get_pfx_metrics_for_game_for_pitcher(self, mlb_id: int, bbref_game_id: str) -> PitchFxMetricsCollection:
        return PitchFxMetricsCollection.from_query_results(
            db.Pitcher_PitchFx_For_Game_All_View.get_pfx_metrics_for_game_for_pitcher(
                self.db_engine, mlb_id, bbref_game_id
            ),
            db.Pitcher_PitchType_For_Game_All_View.get_pfx_metrics_for_game_for_pitcher(
                self.db_engine, mlb_id, bbref_game_id
            ),
        )

    def get_pfx_metrics_for_game_vs_rhb_for_pitcher(self, mlb_id: int, bbref_game_id: str) -> PitchFxMetricsCollection:
        return PitchFxMetricsCollection.from_query_results(
            db.Pitcher_PitchFx_For_Game_vs_RHB_View.get_pfx_metrics_for_game_vs_rhb_for_pitcher(
                self.db_engine, mlb_id, bbref_game_id
            ),
            db.Pitcher_PitchType_For_Game_vs_RHB_View.get_pfx_metrics_for_game_vs_rhb_for_pitcher(
                self.db_engine, mlb_id, bbref_game_id
            ),
        )

    def get_pfx_metrics_for_game_vs_lhb_for_pitcher(self, mlb_id: int, bbref_game_id: str) -> PitchFxMetricsCollection:
        return PitchFxMetricsCollection.from_query_results(
            db.Pitcher_PitchFx_For_Game_vs_LHB_View.get_pfx_metrics_for_game_vs_lhb_for_pitcher(
                self.db_engine, mlb_id, bbref_game_id
            ),
            db.Pitcher_PitchType_For_Game_vs_LHB_View.get_pfx_metrics_for_game_vs_lhb_for_pitcher(
                self.db_engine, mlb_id, bbref_game_id
            ),
        )

    # PLAYER BAT STATS

    def get_bat_stats_for_career_for_player(self, mlb_id: int) -> BatStatsMetrics:
        return db.BatStats_All_View.get_bat_stats_for_career_for_player(self.db_engine, mlb_id)

    def get_bat_stats_by_year_for_player(self, mlb_id: int) -> List[BatStatsMetrics]:
        return db.BatStats_By_Year_View.get_bat_stats_by_year_for_player(self.db_engine, mlb_id)

    def get_bat_stats_by_team_for_player(self, mlb_id: int) -> List[BatStatsMetrics]:
        return db.BatStats_By_Team_View.get_bat_stats_by_team_for_player(self.db_engine, mlb_id)

    def get_bat_stats_by_team_by_year_for_player(self, mlb_id: int) -> List[BatStatsMetrics]:
        return db.BatStats_By_Team_Year_View.get_bat_stats_by_team_by_year_for_player(self.db_engine, mlb_id)

    def get_bat_stats_by_opp_for_player(self, mlb_id: int) -> List[BatStatsMetrics]:
        return db.BatStats_By_Opp_Team_View.get_bat_stats_by_opp_for_player(self.db_engine, mlb_id)

    def get_bat_stats_by_opp_by_year_for_player(self, mlb_id: int) -> List[BatStatsMetrics]:
        return db.BatStats_By_Opp_Team_Year_View.get_bat_stats_by_opp_by_year_for_player(self.db_engine, mlb_id)

    # BATTER PITCHFX STATS

    def get_pfx_metrics_for_career_for_batter(self, mlb_id: int) -> PitchFxMetricsCollection:
        return PitchFxMetricsCollection.from_query_results(
            db.Batter_PitchFx_All_View.get_pfx_metrics_for_career_for_batter(self.db_engine, mlb_id),
            db.Batter_PitchType_All_View.get_pfx_metrics_for_career_for_batter(self.db_engine, mlb_id),
        )

    def get_pfx_metrics_vs_rhp_as_rhb_for_career_for_batter(self, mlb_id: int) -> PitchFxMetricsCollection:
        return PitchFxMetricsCollection.from_query_results(
            db.Batter_PitchFx_vs_RHP_as_RHB_Career_View.get_pfx_metrics_vs_rhp_as_rhb_for_career_for_batter(
                self.db_engine, mlb_id
            ),
            db.Batter_PitchType_vs_RHP_as_RHB_Career_View.get_pfx_metrics_vs_rhp_as_rhb_for_career_for_batter(
                self.db_engine, mlb_id
            ),
        )

    def get_pfx_metrics_vs_rhp_as_lhb_for_career_for_batter(self, mlb_id: int) -> PitchFxMetricsCollection:
        return PitchFxMetricsCollection.from_query_results(
            db.Batter_PitchFx_vs_RHP_as_LHB_Career_View.get_pfx_metrics_vs_rhp_as_lhb_for_career_for_batter(
                self.db_engine, mlb_id
            ),
            db.Batter_PitchType_vs_RHP_as_LHB_Career_View.get_pfx_metrics_vs_rhp_as_lhb_for_career_for_batter(
                self.db_engine, mlb_id
            ),
        )

    def get_pfx_metrics_vs_lhp_as_lhb_for_career_for_batter(self, mlb_id: int) -> PitchFxMetricsCollection:
        return PitchFxMetricsCollection.from_query_results(
            db.Batter_PitchFx_vs_LHP_as_LHB_Career_View.get_pfx_metrics_vs_lhp_as_lhb_for_career_for_batter(
                self.db_engine, mlb_id
            ),
            db.Batter_PitchType_vs_LHP_as_LHB_Career_View.get_pfx_metrics_vs_lhp_as_lhb_for_career_for_batter(
                self.db_engine, mlb_id
            ),
        )

    def get_pfx_metrics_vs_lhp_as_rhb_for_career_for_batter(self, mlb_id: int) -> PitchFxMetricsCollection:
        return PitchFxMetricsCollection.from_query_results(
            db.Batter_PitchFx_vs_LHP_as_RHB_Career_View.get_pfx_metrics_vs_lhp_as_rhb_for_career_for_batter(
                self.db_engine, mlb_id
            ),
            db.Batter_PitchType_vs_LHP_as_RHB_Career_View.get_pfx_metrics_vs_lhp_as_rhb_for_career_for_batter(
                self.db_engine, mlb_id
            ),
        )

    def get_pfx_metrics_for_year_for_batter(self, mlb_id: int, year: int) -> PitchFxMetricsCollection:
        return PitchFxMetricsCollection.from_query_results(
            db.Batter_PitchFx_By_Year_View.get_pfx_metrics_by_year_for_batter(self.db_engine, mlb_id, year),
            db.Batter_PitchType_By_Year_View.get_pfx_metrics_by_year_for_batter(self.db_engine, mlb_id, year),
        )

    def get_pfx_metrics_for_game_for_batter(self, mlb_id: int, bbref_game_id: str) -> PitchFxMetricsCollection:
        return PitchFxMetricsCollection.from_query_results(
            db.Batter_PitchFx_For_Game_All_View.get_pfx_metrics_for_game_for_batter(
                self.db_engine, mlb_id, bbref_game_id
            ),
            db.Batter_PitchType_For_Game_All_View.get_pfx_metrics_for_game_for_batter(
                self.db_engine, mlb_id, bbref_game_id
            ),
        )

    def get_pfx_metrics_vs_rhp_as_rhb_for_game_for_batter(
        self, mlb_id: int, bbref_game_id: str
    ) -> PitchFxMetricsCollection:
        return PitchFxMetricsCollection.from_query_results(
            db.Batter_PitchFx_For_Game_vs_RHP_as_RHB_View.get_pfx_metrics_vs_rhp_as_rhb_for_game_for_batter(
                self.db_engine, mlb_id, bbref_game_id
            ),
            db.Batter_PitchType_For_Game_vs_RHP_as_RHB_View.get_pfx_metrics_vs_rhp_as_rhb_for_game_for_batter(
                self.db_engine, mlb_id, bbref_game_id
            ),
        )

    def get_pfx_metrics_vs_rhp_as_lhb_for_game_for_batter(
        self, mlb_id: int, bbref_game_id: str
    ) -> PitchFxMetricsCollection:
        return PitchFxMetricsCollection.from_query_results(
            db.Batter_PitchFx_For_Game_vs_RHP_as_LHB_View.get_pfx_metrics_vs_rhp_as_lhb_for_game_for_batter(
                self.db_engine, mlb_id, bbref_game_id
            ),
            db.Batter_PitchType_For_Game_vs_RHP_as_LHB_View.get_pfx_metrics_vs_rhp_as_lhb_for_game_for_batter(
                self.db_engine, mlb_id, bbref_game_id
            ),
        )

    def get_pfx_metrics_vs_lhp_as_lhb_for_game_for_batter(
        self, mlb_id: int, bbref_game_id: str
    ) -> PitchFxMetricsCollection:
        return PitchFxMetricsCollection.from_query_results(
            db.Batter_PitchFx_For_Game_vs_LHP_as_LHB_View.get_pfx_metrics_vs_lhp_as_lhb_for_game_for_batter(
                self.db_engine, mlb_id, bbref_game_id
            ),
            db.Batter_PitchType_For_Game_vs_LHP_as_LHB_View.get_pfx_metrics_vs_lhp_as_lhb_for_game_for_batter(
                self.db_engine, mlb_id, bbref_game_id
            ),
        )

    def get_pfx_metrics_vs_lhp_as_rhb_for_game_for_batter(
        self, mlb_id: int, bbref_game_id: str
    ) -> PitchFxMetricsCollection:
        return PitchFxMetricsCollection.from_query_results(
            db.Batter_PitchFx_For_Game_vs_LHP_as_RHB_View.get_pfx_metrics_vs_lhp_as_rhb_for_game_for_batter(
                self.db_engine, mlb_id, bbref_game_id
            ),
            db.Batter_PitchType_For_Game_vs_LHP_as_RHB_View.get_pfx_metrics_vs_lhp_as_rhb_for_game_for_batter(
                self.db_engine, mlb_id, bbref_game_id
            ),
        )


def _sort_and_map_stats_list(stats_list, sort_attr, group_attr):
    stats_list.sort(key=lambda x: getattr(x, sort_attr), reverse=True)
    return {getattr(s, group_attr): s for s in map(_set_team_rank, enumerate(stats_list, start=1))}


def _sort_and_map_team_bat_stats(team_stats_list):
    return _sort_and_map_stats_list(team_stats_list, sort_attr="ops", group_attr="team_id_bbref")


def _sort_and_map_team_pitch_stats(team_stats_list):
    return _sort_and_map_stats_list(team_stats_list, sort_attr="re24_pitch", group_attr="team_id_bbref")


def _set_team_rank(rank_pair):
    (rank, stats) = rank_pair
    stats.rank = rank
    return stats
