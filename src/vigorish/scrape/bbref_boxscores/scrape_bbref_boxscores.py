from vigorish.config.database import DateScrapeStatus
from vigorish.enums import DataSet, ScrapeCondition
from vigorish.scrape.bbref_boxscores.parse_html import parse_bbref_boxscore
from vigorish.scrape.scrape_task import ScrapeTaskABC
from vigorish.status.update_status_bbref_boxscores import update_status_bbref_boxscore
from vigorish.util.dt_format_strings import DATE_ONLY
from vigorish.util.result import Result


class ScrapeBBRefBoxscores(ScrapeTaskABC):
    def __init__(self, db_job, db_session, config, scraped_data):
        self.data_set = DataSet.BBREF_BOXSCORES
        super().__init__(db_job, db_session, config, scraped_data)

    def check_prerequisites(self, game_date):
        bbref_games_for_date = DateScrapeStatus.verify_bbref_daily_dashboard_scraped_for_date(
            self.db_session, game_date
        )
        if bbref_games_for_date:
            return Result.Ok()
        date_str = game_date.strftime(DATE_ONLY)
        error = (
            f"BBref games for date {date_str} have not been scraped, unable to scrape BBref "
            "boxscores until this has been done."
        )
        return Result.Fail(error)

    def check_current_status(self, game_date):
        scraped_bbref_boxscores = DateScrapeStatus.verify_all_bbref_boxscores_scraped_for_date(
            self.db_session, game_date
        )
        if scraped_bbref_boxscores and self.scrape_condition == ScrapeCondition.ONLY_MISSING_DATA:
            return Result.Fail("skip")
        return Result.Ok()

    def parse_html(self, html, url_id, url):
        return parse_bbref_boxscore(html, url)

    def update_status(self, game_date, parsed_data):
        return update_status_bbref_boxscore(self.db_session, parsed_data)
