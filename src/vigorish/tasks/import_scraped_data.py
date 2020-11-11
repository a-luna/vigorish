import subprocess
from pathlib import Path

from events import Events
from halo import Halo

from vigorish.cli.components import get_random_cli_color
from vigorish.config.database import Season
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
                "remove_invalid_pitchfx_logs_start",
                "remove_invalid_pitchfx_logs_complete",
                "import_scraped_data_start",
                "import_scraped_data_complete",
            )
        )

    def execute(self, overwrite_existing=False):
        all_mlb_seasons = [season.year for season in Season.all_regular_seasons(self.db_session)]
        mlb_season_data_map = {
            year: self.local_folder_has_parsed_data_for_season(year) for year in all_mlb_seasons
        }
        if not any(has_data for has_data in mlb_season_data_map.values()):
            self.events.no_scraped_data_found()
            return Result.Ok()
        for year, data_exists in mlb_season_data_map.items():
            if not data_exists:
                continue
            subprocess.run(["clear"])
            result = self.remove_invalid_pitchfx_logs(year)
            if result.failure:
                return result
            result = self.update_all_data_sets(year, overwrite_existing)
            if result.failure:
                return result
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

    def remove_invalid_pitchfx_logs(self, year):
        self.events.remove_invalid_pitchfx_logs_start(year)
        season = Season.find_by_year(self.db_session, year)
        if not season:
            return Result.Fail(f"No data has been scraped for the MLB {year} season.")
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
        self.events.remove_invalid_pitchfx_logs_complete(year)
        return Result.Ok()

    def update_all_data_sets(self, year, overwrite_existing=False):
        for data_set in DataSet:
            if data_set == DataSet.ALL:
                continue
            result = self.update_data_set(data_set, year, overwrite_existing)
            if result.failure:
                return result
        return Result.Ok()

    def update_data_set(self, data_set, year, overwrite_existing=False):
        self.events.import_scraped_data_start(data_set, year)
        scraped_ids = self.scraped_data.get_scraped_ids_from_local_folder(
            VigFile.PARSED_JSON, data_set, year
        )
        if not scraped_ids:
            return Result.Ok()
        spinner = Halo(spinner="dots3", color=get_random_cli_color())
        spinner.text = f"Updating {data_set} for MLB {year}..."
        spinner.start()
        if not overwrite_existing:
            result = self.scraped_data.get_scraped_ids_from_database(data_set, year)
            if result.failure:
                return result
            existing_scraped_ids = result.value
            scraped_ids = list(set(scraped_ids) - set(existing_scraped_ids))
        result = self.update_status_for_data_set(data_set, scraped_ids)
        if result.failure:
            self.events.error_occurred(result.error, data_set, year)
            return result
        self.db_session.commit()
        spinner.succeed(f"Successfully updated {data_set} for MLB {year}!")
        self.events.import_scraped_data_complete(data_set, year)
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
