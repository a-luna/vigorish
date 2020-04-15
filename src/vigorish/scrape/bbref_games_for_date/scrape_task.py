from vigorish.config.database import DateScrapeStatus
from vigorish.enums import DataSet, ScrapeCondition
from vigorish.scrape.bbref_games_for_date.parse_html import parse_bbref_dashboard_page
from vigorish.scrape.scrape_task import ScrapeTaskABC
from vigorish.status.update_status_bbref_games_for_date import (
    update_bbref_games_for_date_single_date,
)
from vigorish.util.result import Result


class ScrapeBBRefGamesForDate(ScrapeTaskABC):
    def __init__(self, db_job, db_session, config, scraped_data):
        self.data_set = DataSet.BBREF_GAMES_FOR_DATE
        super().__init__(db_job, db_session, config, scraped_data)

    def check_prerequisites(self, game_date):
        return Result.Ok()

    def check_current_status(self, game_date):
        bbref_games_for_date = DateScrapeStatus.verify_bbref_daily_dashboard_scraped_for_date(
            self.db_session, game_date
        )
        if bbref_games_for_date and self.scrape_condition == ScrapeCondition.ONLY_MISSING_DATA:
            return Result.Fail("skip")
        return Result.Ok()

    def parse_html(self, html, url_id, url):
        return parse_bbref_dashboard_page(html, url_id, url)

    def update_status(self, game_date, parsed_data):
        return update_bbref_games_for_date_single_date(self.db_session, self.season, parsed_data)
