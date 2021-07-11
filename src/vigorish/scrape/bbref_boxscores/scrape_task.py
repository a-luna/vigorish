import vigorish.database as db
from vigorish.enums import DataSet, ScrapeCondition
from vigorish.scrape.bbref_boxscores.parse_html import parse_bbref_boxscore
from vigorish.scrape.scrape_task import ScrapeTaskABC
from vigorish.status.update_status_bbref_boxscores import update_status_bbref_boxscore
from vigorish.util.dt_format_strings import DATE_ONLY
from vigorish.util.result import Result


class ScrapeBBRefBoxscores(ScrapeTaskABC):
    def __init__(self, app, db_job):
        self.data_set = DataSet.BBREF_BOXSCORES
        super().__init__(app, db_job)

    def check_prerequisites(self, game_date):
        bbref_games_for_date = db.DateScrapeStatus.verify_bbref_daily_dashboard_scraped_for_date(
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
        if self.scrape_condition == ScrapeCondition.ALWAYS:
            return Result.Ok()
        scraped_bbref_boxscores = db.DateScrapeStatus.verify_all_bbref_boxscores_scraped_for_date(
            self.db_session, game_date
        )
        return Result.Ok() if not scraped_bbref_boxscores else Result.Fail("skip")

    def parse_html(self, url_details):
        return parse_bbref_boxscore(url_details.html, url_details.url, url_details.url_id)

    def update_status(self, parsed_data):
        result = update_status_bbref_boxscore(self.db_session, parsed_data)
        if result.failure:
            return result
        self.db_session.commit()
        return Result.Ok()
