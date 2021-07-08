from collections import defaultdict
from pathlib import Path

from events import Events

import vigorish.database as db
from vigorish.enums import DataSet, VigFile
from vigorish.status.update_status_bbref_boxscores import update_status_bbref_boxscore_list
from vigorish.status.update_status_bbref_games_for_date import update_bbref_games_for_date_list
from vigorish.status.update_status_brooks_games_for_date import (
    update_brooks_games_for_date_list,
)
from vigorish.status.update_status_brooks_pitch_logs import (
    update_status_brooks_pitch_logs_for_game_list,
)
from vigorish.status.update_status_brooks_pitchfx import update_status_brooks_pitchfx_log_list
from vigorish.status.update_status_combined_data import update_status_combined_data_list
from vigorish.tasks.base import Task
from vigorish.util.result import Result

IMPORT_DATA_SET_ORDER = {
    1: (DataSet.BBREF_GAMES_FOR_DATE, update_bbref_games_for_date_list),
    2: (DataSet.BROOKS_GAMES_FOR_DATE, update_brooks_games_for_date_list),
    3: (DataSet.BBREF_BOXSCORES, update_status_bbref_boxscore_list),
    4: (DataSet.BROOKS_PITCH_LOGS, update_status_brooks_pitch_logs_for_game_list),
    5: (DataSet.BROOKS_PITCHFX, update_status_brooks_pitchfx_log_list),
}


class ImportScrapedDataTask(Task):
    def __init__(self, app):
        super().__init__(app)
        self.events = Events(
            (
                "error_occurred",
                "no_scraped_data_found",
                "search_local_files_start",
                "search_local_files_complete",
                "import_scraped_data_start",
                "import_scraped_data_complete",
                "import_scraped_data_for_year_start",
                "import_scraped_data_for_year_complete",
                "import_scraped_data_set_start",
                "import_scraped_data_set_complete",
                "import_combined_game_data_start",
                "import_combined_game_data_complete",
            )
        )

    def execute(self, import_seasons=None, overwrite_existing=False):
        mlb_season_map = self._get_mlb_season_scraped_data_map(import_seasons)
        if not any(season["has_data"] for season in mlb_season_map.values()):
            self.events.no_scraped_data_found()
            return Result.Ok()
        self._remove_invalid_pitchfx_logs(mlb_season_map)
        return self._import_scraped_data(mlb_season_map, overwrite_existing)

    def _get_mlb_season_scraped_data_map(self, import_seasons):
        self.events.search_local_files_start()
        if import_seasons:
            seasons = [db.Season.find_by_year(self.db_session, year) for year in import_seasons]
        else:
            seasons = db.Season.get_all_regular_seasons(self.db_session)
        return {
            season.year: {
                "has_data": self._local_folder_has_parsed_data_for_season(season.year),
                "season": season,
            }
            for season in seasons
        }

    def _local_folder_has_parsed_data_for_season(self, year):
        for data_set in DataSet:
            scraped_ids = self.scraped_data.get_scraped_ids_from_local_folder(VigFile.PARSED_JSON, data_set, year)
            if scraped_ids:
                return True
        return False

    def _remove_invalid_pitchfx_logs(self, mlb_season_map):
        for season in mlb_season_map.values():
            if season["has_data"]:
                invalid_ids = self._get_invalid_pitchfx_log_ids_for_season(season["season"])
                if invalid_ids:
                    self._delete_invalid_pitchfx_json_files(season["season"], invalid_ids)
        self.events.search_local_files_complete()

    def _get_invalid_pitchfx_log_ids_for_season(self, season):
        valid_ids = []
        for game_date in season.get_date_range():
            games_for_date = self.scraped_data.get_brooks_games_for_date(game_date)
            if not games_for_date:
                continue
            valid_ids.extend(games_for_date.all_pitch_app_ids_for_date)
        local_ids = self.scraped_data.get_scraped_ids_from_local_folder(
            VigFile.PARSED_JSON, DataSet.BROOKS_PITCHFX, season.year
        )
        return list(set(local_ids) - set(valid_ids))

    def _delete_invalid_pitchfx_json_files(self, season, invalid_ids):
        pfx_folderpath = self.scraped_data.get_local_folderpath(
            VigFile.PARSED_JSON, DataSet.BROOKS_PITCHFX, season.year
        )
        for pitch_app_id in invalid_ids:
            invalid_pitchfx_log = Path(pfx_folderpath).joinpath(f"{pitch_app_id}.json")
            if invalid_pitchfx_log.exists():
                invalid_pitchfx_log.unlink()

    def _import_scraped_data(self, mlb_season_map, overwrite_existing):
        self.events.import_scraped_data_start()
        error_dict = defaultdict(list)
        for season in mlb_season_map.values():
            if not season["has_data"]:
                continue
            result = self._update_all_data_sets(season["season"], overwrite_existing)
            if result.failure:
                error_dict[season["season"].year].append(result.error)
            result = self._update_combined_data(season["season"], overwrite_existing)
            if result.failure:
                error_dict[season["season"].year].append(result.error)
        self.events.import_scraped_data_complete()
        error_dict = self._check_error_dict(error_dict)
        return Result.Ok() if not error_dict else Result.Fail(self._create_error_message_for_task(error_dict))

    def _create_error_message_for_task(self, error_dict):
        error_list = [f"{year}: " + "\n".join(errors) for year, errors in error_dict.items()]
        return "\n".join(error_list)

    def _update_all_data_sets(self, season, overwrite_existing=False):
        error_dict = {}
        self.events.import_scraped_data_for_year_start(season.year)
        for i in IMPORT_DATA_SET_ORDER.keys():
            (data_set, update_data_set) = IMPORT_DATA_SET_ORDER[i]
            self.events.import_scraped_data_set_start(data_set, season.year)
            scraped_ids = self._get_scraped_ids_for_data_set(data_set, season, overwrite_existing)
            result = update_data_set(self.scraped_data, self.db_session, scraped_ids)
            self.db_session.commit()
            self.events.import_scraped_data_set_complete(data_set, season.year)
            if result.failure:
                error_dict[data_set] = result.error
                continue
        self.events.import_scraped_data_for_year_complete(season.year)
        return Result.Ok() if not error_dict else Result.Fail(self._create_error_message_for_season(error_dict))

    def _create_error_message_for_season(self, error_dict):
        error_list = [f"{data_set.name}: {error_ids}" for data_set, error_ids in error_dict.items()]
        return "\n".join(error_list)

    def _get_scraped_ids_for_data_set(self, data_set, season, overwrite_existing):
        scraped_ids = self.scraped_data.get_scraped_ids_from_local_folder(VigFile.PARSED_JSON, data_set, season.year)
        if overwrite_existing:
            return scraped_ids
        existing_scraped_ids = self.scraped_data.get_scraped_ids_from_database(data_set, season)
        return list(set(scraped_ids) - set(existing_scraped_ids))

    def _update_combined_data(self, season, overwrite_existing=False):
        self.events.import_combined_game_data_start(season.year)
        combined_ids = self.scraped_data.get_scraped_ids_from_local_folder(
            VigFile.COMBINED_GAME_DATA, DataSet.ALL, season.year
        )
        if not combined_ids:
            self.events.import_combined_game_data_complete(season.year)
            return Result.Ok()
        if not overwrite_existing:
            season_report = self.app.audit_report.get(season.year, {})
            if season_report:
                existing_combined_ids = (
                    season_report["successful"] + season_report["pfx_error"] + season_report["invalid_pfx"]
                )
                combined_ids = list(set(combined_ids) - set(existing_combined_ids))
        result = update_status_combined_data_list(self.scraped_data, self.db_session, combined_ids)
        self.events.import_combined_game_data_complete(season.year)
        return result

    def _check_error_dict(self, error_dict):
        for year in list(error_dict.keys())[:]:
            if not error_dict[year]:
                del error_dict[year]
