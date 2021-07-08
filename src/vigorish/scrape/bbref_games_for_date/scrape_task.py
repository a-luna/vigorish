import vigorish.database as db
from vigorish.enums import DataSet, ScrapeCondition
from vigorish.scrape.bbref_games_for_date.parse_html import parse_bbref_dashboard_page
from vigorish.scrape.scrape_task import ScrapeTaskABC
from vigorish.status.update_status_bbref_games_for_date import (
    update_bbref_games_for_date_single_date,
)
from vigorish.util.result import Result


class ScrapeBBRefGamesForDate(ScrapeTaskABC):
    def __init__(self, app, db_job):
        self.data_set = DataSet.BBREF_GAMES_FOR_DATE
        super().__init__(app, db_job)

    def check_prerequisites(self, game_date):
        return Result.Ok()

    def check_current_status(self, game_date):
        if self.scrape_condition == ScrapeCondition.ALWAYS:
            return Result.Ok()
        bbref_games_for_date = db.DateScrapeStatus.verify_bbref_daily_dashboard_scraped_for_date(
            self.db_session, game_date
        )
        return Result.Ok() if not bbref_games_for_date else Result.Fail("skip")

    def parse_html(self, url_details):
        return parse_bbref_dashboard_page(url_details.html, url_details.url_id, url_details.url)

    def update_status(self, parsed_data):
        result = update_bbref_games_for_date_single_date(self.db_session, parsed_data)
        if result.failure:
            return result
        self.db_session.commit()
        return Result.Ok()
