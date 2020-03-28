from vigorish.config.database import DateScrapeStatus
from vigorish.enums import DataSet, PythonScrapeTool, ScrapeCondition
from vigorish.scrape.brooks_games_for_date.parse_html import parse_brooks_dashboard_page
from vigorish.scrape.scrape_task import ScrapeTaskABC
from vigorish.status.update_status_brooks_games_for_date import (
    update_brooks_games_for_date_single_date,
)
from vigorish.util.result import Result


class ScrapeBrooksGamesForDate(ScrapeTaskABC):
    def __init__(self, db_job, db_session, config, scraped_data, driver):
        self.data_set = DataSet.BROOKS_GAMES_FOR_DATE
        super().__init__(db_job, db_session, config, scraped_data, driver)

    def check_prerequisites(self, game_date):
        scraped_bbref_games_for_date = DateScrapeStatus.verify_bbref_daily_dashboard_scraped_for_date(
            self.db_session, game_date
        )
        if scraped_bbref_games_for_date:
            return Result.Ok()
        date_str = game_date.strftime(DATE_ONLY)
        error = (
            f"BBref games for date ({date_str}) have not been scraped, unable to scrape Brooks "
            f"games for date ({date_str}) until this has been done."
        )
        return Result.Fail(error)

    def check_current_status(self, game_date):
        scraped_brooks_games_for_date = DateScrapeStatus.verify_brooks_daily_dashboard_scraped_for_date(
            self.db_session, game_date
        )
        if (
            scraped_brooks_games_for_date
            and self.scrape_condition == ScrapeCondition.ONLY_MISSING_DATA
        ):
            return Result.Fail("skip")
        return Result.Ok()

    def scrape_html_with_requests_selenium(self, missing_html):
        self.python_scrape_tool = PythonScrapeTool.SELENIUM
        super().scrape_html_with_requests_selenium(missing_html)

    def parse_html(self, page_source, url_id, url):
        result = self.scraped_data.get_bbref_games_for_date(url_id)
        if result.failure:
            return result
        bbref_games_for_date = result.value
        return parse_brooks_dashboard_page(
            self.db_session, page_source, url_id, url, bbref_games_for_date
        )

    def update_status(self, game_date, parsed_data):
        return update_brooks_games_for_date_single_date(
            self.db_session, self.season, parsed_data, game_date
        )