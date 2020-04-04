from halo import Halo
from lxml import html

from vigorish.config.database import DateScrapeStatus
from vigorish.constants import JOB_SPINNER_COLORS
from vigorish.enums import DataSet, PythonScrapeTool, ScrapeCondition, JobStatus
from vigorish.scrape.brooks_pitch_logs.models.pitch_logs_for_game import BrooksPitchLogsForGame
from vigorish.scrape.brooks_pitch_logs.parse_html import parse_pitch_log
from vigorish.scrape.scrape_task import ScrapeTaskABC
from vigorish.status.update_status_brooks_pitch_logs import (
    update_status_brooks_pitch_logs_for_game,
)
from vigorish.util.dt_format_strings import DATE_ONLY_2
from vigorish.util.result import Result


class ScrapeBrooksPitchLogs(ScrapeTaskABC):
    def __init__(self, db_job, db_session, config, scraped_data, driver):
        self.data_set = DataSet.BROOKS_PITCH_LOGS
        super().__init__(db_job, db_session, config, scraped_data, driver)

    def check_prerequisites(self, game_date):
        brooks_games_for_date = DateScrapeStatus.verify_brooks_daily_dashboard_scraped_for_date(
            self.db_session, game_date
        )
        if brooks_games_for_date:
            return Result.Ok()
        date_str = game_date.strftime(DATE_ONLY_2)
        error = (
            f"Brooks games for date {date_str} have not been scraped, unable to scrape Brooks "
            "pitch logs until this has been done."
        )
        return Result.Fail(error)

    def check_current_status(self, game_date):
        scraped_brooks_pitch_logs = DateScrapeStatus.verify_all_brooks_pitch_logs_scraped_for_date(
            self.db_session, game_date
        )
        if (
            scraped_brooks_pitch_logs
            and self.scrape_condition == ScrapeCondition.ONLY_MISSING_DATA
        ):
            return Result.Fail("skip")
        return Result.Ok()

    def scrape_html_with_requests_selenium(self, missing_html):
        self.python_scrape_tool = PythonScrapeTool.REQUESTS
        super().scrape_html_with_requests_selenium(missing_html)

    def parse_data_from_scraped_html(self):
        self.db_job.status = JobStatus.PARSING
        self.db_session.commit()
        spinner = Halo(color=JOB_SPINNER_COLORS[self.data_set], spinner="dots3")
        spinner.text = "Parsing HTML..."
        spinner.start()
        parsed = 0
        for game_date in self.date_range:
            result = self.scraped_data.get_brooks_games_for_date(game_date)
            if result.failure:
                spinner.fail(f"Error! {result.error}")
                return result
            brooks_games_for_date = result.value
            for game in brooks_games_for_date.games:
                pitch_logs_for_game = BrooksPitchLogsForGame()
                pitch_logs_for_game.bb_game_id = game.bb_game_id
                pitch_logs_for_game.bbref_game_id = game.bbref_game_id
                pitch_logs_for_game.pitch_log_count = game.pitcher_appearance_count
                scraped_pitch_logs = []
                for pitcher_id, url in game.pitcher_appearance_dict.items():
                    spinner.text = (
                        f"Parsing Pitch Logs for {game.bbref_game_id}... "
                        f"{parsed / float(self.total_urls):.0%} ({parsed}/{self.total_urls} URLs)"
                    )
                    pitch_app_id = f"{game.bbref_game_id}_{pitcher_id}"
                    filepath = self.scraped_data.get_html(self.data_set, pitch_app_id)
                    if not filepath:
                        error = f"Failed to retrieve HTML for pitch app: {pitch_app_id}"
                        spinner.fail(error)
                        return Result.Fail(error)
                    page_source = html.fromstring(filepath.read_text(), base_url=url)
                    result = parse_pitch_log(page_source, game, pitcher_id, url)
                    if result.failure:
                        spinner.fail(f"Error! {result.error} (ID: {pitch_app_id})")
                        return result
                    pitch_log = result.value
                    scraped_pitch_logs.append(pitch_log)
                    parsed += 1
                    spinner.text = (
                        f"Parsing Pitch Logs for {game.bbref_game_id}... "
                        f"{parsed / float(self.total_urls):.0%} ({parsed}/{self.total_urls} URLs)"
                    )
                spinner.text = (
                    f"Updating Pitch Logs for {game.bbref_game_id}... "
                    f"{parsed / float(self.total_urls):.0%} ({parsed}/{self.total_urls} URLs)"
                )
                pitch_logs_for_game.pitch_logs = scraped_pitch_logs
                result = self.scraped_data.save_json(self.data_set, pitch_logs_for_game)
                if result.failure:
                    spinner.fail(f"Error! {result.error} (ID: {game.bbref_game_id})")
                    return result
                result = self.update_status(game_date, pitch_logs_for_game)
                if result.failure:
                    spinner.fail(f"Error! {result.error} (ID: {game.bbref_game_id})")
                    return result
        spinner.succeed("HTML Parsed")
        return Result.Ok()

    def parse_html(self, page_source, url_id, url):
        pass

    def update_status(self, game_date, parsed_data):
        return update_status_brooks_pitch_logs_for_game(self.db_session, game_date, parsed_data)
