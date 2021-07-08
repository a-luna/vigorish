import vigorish.database as db
from vigorish.enums import DataSet, ScrapeCondition
from vigorish.scrape.brooks_pitch_logs.models.pitch_logs_for_game import BrooksPitchLogsForGame
from vigorish.scrape.brooks_pitch_logs.parse_html import parse_pitch_log
from vigorish.scrape.scrape_task import ScrapeTaskABC
from vigorish.status.update_status_brooks_pitch_logs import update_status_brooks_pitch_logs_for_game
from vigorish.util.dt_format_strings import DATE_MONTH_NAME, DATE_ONLY_2
from vigorish.util.result import Result


class ScrapeBrooksPitchLogs(ScrapeTaskABC):
    def __init__(self, app, db_job):
        self.data_set = DataSet.BROOKS_PITCH_LOGS
        self.req_data_set = DataSet.BROOKS_GAMES_FOR_DATE
        super().__init__(app, db_job)

    def check_prerequisites(self, game_date):
        brooks_games_for_date = db.DateScrapeStatus.verify_brooks_daily_dashboard_scraped_for_date(
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
        if self.scrape_condition == ScrapeCondition.ALWAYS:
            return Result.Ok()
        scraped_brooks_pitch_logs = db.DateScrapeStatus.verify_all_brooks_pitch_logs_scraped_for_date(
            self.db_session, game_date
        )
        return Result.Ok() if not scraped_brooks_pitch_logs else Result.Fail("skip")

    def parse_scraped_html(self):
        parsed = 0
        for game_date in self.date_range:
            brooks_games_for_date = self.scraped_data.get_brooks_games_for_date(game_date)
            if not brooks_games_for_date:
                date_str = game_date.strftime(DATE_MONTH_NAME)
                return Result.Fail(f"Failed to retrieve {self.req_data_set} (Date: {date_str})")
            for game in brooks_games_for_date.games:
                if game.bbref_game_id not in self.url_tracker.parse_url_ids:
                    continue
                if game.might_be_postponed:
                    continue
                self.spinner.text = self.url_tracker.parse_html_report(parsed, game.bbref_game_id)
                pitch_logs_for_game = BrooksPitchLogsForGame()
                pitch_logs_for_game.bb_game_id = game.bb_game_id
                pitch_logs_for_game.bbref_game_id = game.bbref_game_id
                pitch_logs_for_game.pitch_log_count = game.pitcher_appearance_count
                scraped_pitch_logs = []
                for pitcher_id, url in game.pitcher_appearance_dict.items():
                    pitch_app_id = f"{game.bbref_game_id}_{pitcher_id}"
                    html = self.url_tracker.get_html(pitch_app_id)
                    result = parse_pitch_log(html, game, pitcher_id, url)
                    if result.failure:
                        return result
                    pitch_log = result.value
                    scraped_pitch_logs.append(pitch_log)
                pitch_logs_for_game.pitch_logs = scraped_pitch_logs
                result = self.scraped_data.save_json(self.data_set, pitch_logs_for_game)
                if result.failure:
                    return result
                result = self.update_status(pitch_logs_for_game)
                if result.failure:
                    return result
                parsed += 1
                self.spinner.text = self.url_tracker.parse_html_report(parsed, game.bbref_game_id)
                self.db_session.commit()
        return Result.Ok()

    def parse_html(self, url_details):
        pass

    def update_status(self, parsed_data):
        result = update_status_brooks_pitch_logs_for_game(self.db_session, parsed_data)
        if result.failure:
            return result
        self.db_session.commit()
        return Result.Ok()
