from lxml import html

from vigorish.config.database import DateScrapeStatus
from vigorish.enums import DataSet, ScrapeCondition, JobStatus
from vigorish.scrape.brooks_pitch_logs.models.pitch_logs_for_game import BrooksPitchLogsForGame
from vigorish.scrape.brooks_pitch_logs.parse_html import parse_pitch_log
from vigorish.scrape.scrape_task import ScrapeTaskABC
from vigorish.status.update_status_brooks_pitch_logs import (
    update_status_brooks_pitch_logs_for_game,
)
from vigorish.util.dt_format_strings import DATE_ONLY_2
from vigorish.util.result import Result


class ScrapeBrooksPitchLogs(ScrapeTaskABC):
    def __init__(self, db_job, db_session, config, scraped_data):
        self.data_set = DataSet.BROOKS_PITCH_LOGS
        super().__init__(db_job, db_session, config, scraped_data)

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

    def parse_data_from_scraped_html(self):
        self.db_job.status = JobStatus.PARSING
        self.db_session.commit()
        parsed = 0
        for game_date in self.date_range:
            result = self.scraped_data.get_brooks_games_for_date(game_date)
            if result.failure:
                return result
            brooks_games_for_date = result.value
            for game in brooks_games_for_date.games:
                pitch_logs_for_game = BrooksPitchLogsForGame()
                pitch_logs_for_game.bb_game_id = game.bb_game_id
                pitch_logs_for_game.bbref_game_id = game.bbref_game_id
                pitch_logs_for_game.pitch_log_count = game.pitcher_appearance_count
                percent_complete = parsed / float(self.total_urls)
                self.spinner.text = (
                    f"Parsing Pitch Logs for {game.bbref_game_id}... {percent_complete:.0%} "
                    f"({parsed}/{self.total_urls} URLs)"
                )
                scraped_pitch_logs = []
                for pitcher_id, url in game.pitcher_appearance_dict.items():
                    pitch_app_id = f"{game.bbref_game_id}_{pitcher_id}"
                    filepath = self.scraped_data.get_html(self.data_set, pitch_app_id)
                    if not filepath:
                        return Result.Fail(f"Failed to locate HTML for pitch app: {pitch_app_id}")
                    page_content = html.fromstring(filepath.read_text(), base_url=url)
                    result = parse_pitch_log(page_content, game, pitcher_id, url)
                    if result.failure:
                        return result
                    pitch_log = result.value
                    scraped_pitch_logs.append(pitch_log)
                    parsed += 1
                    percent_complete = parsed / float(self.total_urls)
                    self.spinner.text = (
                        f"Parsing Pitch Logs for {game.bbref_game_id}... {percent_complete:.0%} "
                        f"({parsed}/{self.total_urls} URLs)"
                    )
                self.spinner.text = (
                    f"Updating Pitch Logs for {game.bbref_game_id}... "
                    f"{parsed / float(self.total_urls):.0%} ({parsed}/{self.total_urls} URLs)"
                )
                pitch_logs_for_game.pitch_logs = scraped_pitch_logs
                result = self.scraped_data.save_json(self.data_set, pitch_logs_for_game)
                if result.failure:
                    return result
                result = self.update_status(game_date, pitch_logs_for_game)
                if result.failure:
                    return result
        return Result.Ok()

    def parse_html(self, page_content, url_id, url):
        pass

    def update_status(self, game_date, parsed_data):
        return update_status_brooks_pitch_logs_for_game(self.db_session, game_date, parsed_data)
