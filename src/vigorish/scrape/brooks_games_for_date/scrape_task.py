import vigorish.database as db
from vigorish.enums import DataSet, ScrapeCondition
from vigorish.scrape.brooks_games_for_date.parse_html import parse_brooks_dashboard_page
from vigorish.scrape.scrape_task import ScrapeTaskABC
from vigorish.status.update_status_brooks_games_for_date import (
    update_brooks_games_for_date_single_date,
)
from vigorish.util.dt_format_strings import DATE_ONLY
from vigorish.util.result import Result


class ScrapeBrooksGamesForDate(ScrapeTaskABC):
    def __init__(self, app, db_job):
        self.data_set = DataSet.BROOKS_GAMES_FOR_DATE
        super().__init__(app, db_job)

    def check_prerequisites(self, game_date):
        bbref_games_for_date = db.DateScrapeStatus.verify_bbref_daily_dashboard_scraped_for_date(
            self.db_session, game_date
        )
        if bbref_games_for_date:
            return Result.Ok()
        date_str = game_date.strftime(DATE_ONLY)
        error = (
            f"BBref games for date ({date_str}) have not been scraped, unable to scrape Brooks "
            f"games for date ({date_str}) until this has been done."
        )
        return Result.Fail(error)

    def check_current_status(self, game_date):
        if self.scrape_condition == ScrapeCondition.ALWAYS:
            return Result.Ok()
        brooks_games_for_date = db.DateScrapeStatus.verify_brooks_daily_dashboard_scraped_for_date(
            self.db_session, game_date
        )
        return Result.Ok() if not brooks_games_for_date else Result.Fail("skip")

    def parse_html(self, url_details):
        bbref_games_for_date = self.scraped_data.get_bbref_games_for_date(url_details.url_id)
        if not bbref_games_for_date:
            return Result.Fail(f"Failed to retrieve {self.data_set} (URL ID: {url_details.url_id})")
        return parse_brooks_dashboard_page(
            self.db_session,
            url_details.html,
            url_details.url_id,
            url_details.url,
            bbref_games_for_date,
        )

    def update_status(self, parsed_data):
        return update_brooks_games_for_date_single_date(self.db_session, self.season, parsed_data)
