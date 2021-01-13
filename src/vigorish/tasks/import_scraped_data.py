# import subprocess
from pathlib import Path

from events import Events

from vigorish.database import Season
from vigorish.enums import DataSet, VigFile
from vigorish.status.update_status_bbref_boxscores import update_status_bbref_boxscore_list
from vigorish.status.update_status_bbref_games_for_date import update_bbref_games_for_date_list
from vigorish.status.update_status_brooks_games_for_date import (
    update_status_brooks_games_for_date_list,
)
from vigorish.status.update_status_brooks_pitch_logs import (
    update_status_brooks_pitch_logs_for_game_list,
)
from vigorish.status.update_status_brooks_pitchfx import update_status_brooks_pitchfx_log_list
from vigorish.tasks.base import Task
from vigorish.util.result import Result


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
            )
        )

    def execute(self, overwrite_existing=False):
        self.events.search_local_files_start()
        all_mlb_seasons = Season.all_regular_seasons(self.db_session)
        mlb_season_map = {
            season.year: {
                "has_data": self.local_folder_has_parsed_data_for_season(season.year),
                "season": season,
            }
            for season in all_mlb_seasons
        }
        if not any(season["has_data"] for season in mlb_season_map.values()):
            self.events.no_scraped_data_found()
            return Result.Ok()
        for season in mlb_season_map.values():
            if not season["has_data"]:
                continue
            result = self.remove_invalid_pitchfx_logs(season["season"])
            if result.failure:
                return result
        self.events.search_local_files_complete()
        self.events.import_scraped_data_start()
        for season in mlb_season_map.values():
            if not season["has_data"]:
                continue
            self.events.import_scraped_data_for_year_start(season["season"].year)
            result = self.update_all_data_sets(season["season"], overwrite_existing)
            if result.failure:
                return result
            self.events.import_scraped_data_for_year_complete(season["season"].year)
        self.events.import_scraped_data_complete()
        return Result.Ok()

    def local_folder_has_parsed_data_for_season(self, year):
        for data_set in DataSet:
            if data_set == DataSet.ALL:
                continue
            scraped_ids = self.scraped_data.get_scraped_ids_from_local_folder(
                VigFile.PARSED_JSON, data_set, year
            )
            if scraped_ids:
                return True
        return False

    def remove_invalid_pitchfx_logs(self, season):
        valid_ids = []
        for game_date in season.get_date_range():
            games_for_date = self.scraped_data.get_brooks_games_for_date(game_date)
            if not games_for_date:
                continue
            valid_ids.extend(games_for_date.all_pitch_app_ids_for_date)
        folder_ids = self.scraped_data.get_scraped_ids_from_local_folder(
            VigFile.PARSED_JSON, DataSet.BROOKS_PITCHFX, season.year
        )
        invalid_ids = list(set(folder_ids) - set(valid_ids))
        if not invalid_ids:
            return Result.Ok()
        pfx_folderpath = self.scraped_data.get_local_folderpath(
            VigFile.PARSED_JSON, DataSet.BROOKS_PITCHFX, season.year
        )
        for pitch_app_id in invalid_ids:
            invalid_pitchfx_log = Path(pfx_folderpath).joinpath(f"{pitch_app_id}.json")
            if invalid_pitchfx_log.exists():
                invalid_pitchfx_log.unlink()
        return Result.Ok()

    def update_all_data_sets(self, season, overwrite_existing=False):
        for data_set in DataSet:
            if data_set == DataSet.ALL:
                continue
            self.events.import_scraped_data_set_start(data_set, season.year)
            result = self.update_data_set(data_set, season, overwrite_existing)
            if result.failure:
                return result
            self.events.import_scraped_data_set_complete(data_set, season.year)
        return Result.Ok()

    def update_data_set(self, data_set, season, overwrite_existing=False):
        scraped_ids = self.scraped_data.get_scraped_ids_from_local_folder(
            VigFile.PARSED_JSON, data_set, season.year
        )
        if not scraped_ids:
            return Result.Ok()
        if not overwrite_existing:
            existing_scraped_ids = self.scraped_data.get_scraped_ids_from_database(data_set, season)
            scraped_ids = list(set(scraped_ids) - set(existing_scraped_ids))
        result = self.update_status_for_data_set(data_set, scraped_ids)
        if result.failure and "scrape_status_game does not contain an entry" not in result.error:
            self.events.error_occurred(result.error, data_set, season.year)
            return result
        self.db_session.commit()
        return Result.Ok()

    def update_status_for_data_set(self, data_set, scraped_ids):
        update_status_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: update_status_brooks_games_for_date_list,
            DataSet.BROOKS_PITCH_LOGS: update_status_brooks_pitch_logs_for_game_list,
            DataSet.BROOKS_PITCHFX: update_status_brooks_pitchfx_log_list,
            DataSet.BBREF_GAMES_FOR_DATE: update_bbref_games_for_date_list,
            DataSet.BBREF_BOXSCORES: update_status_bbref_boxscore_list,
        }
        return update_status_dict[data_set](self.scraped_data, self.db_session, scraped_ids)
