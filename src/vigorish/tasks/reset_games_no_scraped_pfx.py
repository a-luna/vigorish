from halo import Halo

from vigorish.cli.util import get_random_cli_color, get_random_dots_spinner
from vigorish.config.database import (
    GameScrapeStatus,
    Season,
    PitchAppScrapeStatus,
    Season_Game_PitchApp_View as SeasonView,
)
from vigorish.enums import DataSet
from vigorish.util.result import Result


class RemovePitchFxLogsWithoutData:
    season: Season
    spinner: Halo

    def __init__(self, db_engine, db_session, scraped_data):
        self.db_session = db_session
        self.scraped_data = scraped_data
        self.game_ids = []
        self.results = {}
        self.results["scrape_dates"] = set()
        self.results["scrape_pitch_app_ids"] = []

    @property
    def year(self):
        return self.season.year if self.season else None

    def execute(self, year):
        result = (
            self.initialize_spinner()
            .on_success(self.get_game_ids_without_pitchfx_data, year)
            .on_success(self.remove_pitchfx_logs_with_no_data)
        )
        if result.failure:
            self.spinner.stop()
            return result
        self.results["scrape_dates"] = sorted(self.results["scrape_dates"])
        self.results["scrape_pitch_app_ids"] = sorted(self.results["scrape_pitch_app_ids"])
        self.display_results()
        return Result.Ok(self.results)

    def initialize_spinner(self):
        self.spinner = Halo(color=get_random_cli_color(), spinner=get_random_dots_spinner())
        self.spinner.text = f"Finding all games in MLB {self.year} season with no PitchFX data..."
        self.spinner.start()
        return Result.Ok()

    def get_game_ids_without_pitchfx_data(self, year):
        self.game_ids = SeasonView.get_all_bbref_game_ids_no_pitchfx_data_for_any_pitch_apps(year)
        if not self.game_ids:
            return Result.Fail(f"All games in MLB {year} have scraped some amount of PitchFX data")
        return Result.Ok()

    def remove_pitchfx_logs_with_no_data(self):
        for num, game_id in enumerate(self.game_ids, start=1):
            self.update_spinner_current_game_id(game_id, num)
            result = self.reset_game_scrape_status(game_id)
            if result.failure:
                return result
            result = self.get_pitch_app_ids_for_game(game_id)
            if result.failure:
                return result
            pitch_app_ids = result.value
            for pitch_app_id in pitch_app_ids:
                result = self.get_pitch_app_status(pitch_app_id)
                if result.failure:
                    return result
                pitch_app_status = result.value
                if pitch_app_status.no_pitchfx_data:
                    result = self.remove_all_html_for_pitch_app(pitch_app_id)
                    if result.failure:
                        return result
                    self.results["scrape_pitch_app_ids"].append(pitch_app_id)
                self.db_session.delete(pitch_app_status)
        self.db_session.commit()
        self.spinner.stop()
        return Result.Ok()

    def update_spinner_current_game_id(self, game_id, num):
        self.spinner.text = (
            f"Preparing to re-scrape PitchFx data for {game_id} ({num}/{len(self.game_ids)})"
        )

    def reset_game_scrape_status(self, game_id):
        game_status = GameScrapeStatus.find_by_bbref_game_id(self.db_session, game_id)
        if not game_status:
            return Result.Fail(f"Error! Failed to retrieve GameScrapeStatus for {game_id}")
        self.results["scrape_dates"].add(game_status.game_date)
        setattr(game_status, "scraped_brooks_pitch_logs", 0)
        return Result.Ok()

    def get_pitch_app_ids_for_game(self, game_id):
        pitch_app_ids = PitchAppScrapeStatus.get_all_pitch_app_ids_for_game(
            self.db_session, game_id
        )
        if not pitch_app_ids:
            return Result.Fail(f"Error! Failed to retrieve pitch app IDs for {game_id}")
        return Result.Ok(pitch_app_ids)

    def get_pitch_app_status(self, pitch_app_id):
        pitch_app_status = PitchAppScrapeStatus.find_by_pitch_app_id(self.db_session, pitch_app_id)
        if not pitch_app_status:
            return Result.Fail(f"pitch_app_status does not exist for ID: {pitch_app_id}")
        return Result.Ok(pitch_app_status)

    def remove_all_html_for_pitch_app(self, pitch_app_id):
        self.scraped_data.delete_html(DataSet.BROOKS_PITCH_LOGS, pitch_app_id)
        self.scraped_data.delete_html(DataSet.BROOKS_PITCHFX, pitch_app_id)
        return Result.Ok()

    def display_results(self):
        pitch_apps_plural = (
            "pitch appearances"
            if len(self.results["scrape_pitch_app_ids"]) > 1
            else "pitch appearance"
        )
        games_plural = "total games" if len(self.game_ids) > 1 else "total game"
        self.results["message"] = (
            "Successfully removed PitchFX HTML files and reset scrape status for "
            f"{len(self.results['scrape_pitch_app_ids'])} {pitch_apps_plural} from "
            f"{len(self.game_ids)} {games_plural}"
        )
