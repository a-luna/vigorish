from app.main.models.status_date import DateScrapeStatus
from app.main.scrape.bbref.scrape_bbref_games_for_date import scrape_bbref_games_for_date
from app.main.scrape.brooks.scrape_brooks_games_for_date import (
    scrape_brooks_games_for_date,
)
from app.main.status.update_status_bbref_games_for_date import (
    update_bbref_games_for_date_single_date,
)
from app.main.status.update_status_brooks_games_for_date import (
    update_brooks_games_for_date_single_date,
)
from app.main.tasks.base_task import BaseTask
from app.main.util.s3_helper import (
    get_bbref_games_for_date_from_s3,
    upload_bbref_games_for_date,
    upload_brooks_games_for_date,
)


class FindAllGamesForDateTask(BaseTask):
    key_name = "find_games_for_date"
    display_name = "Games for date (bbref.com)"

    def execute(self, scrape_date):
        result = self.get_bbref_games_for_date(scrape_date)
        if result.failure:
            return result
        bbref_games_for_date = result.value
        return self.get_brooks_games_for_date(scrape_date, bbref_games_for_date)

    def get_bbref_games_for_date(self, scrape_date):
        scraped_bbref_games_for_date = DateScrapeStatus.verify_bbref_daily_dashboard_scraped_for_date(
            self.db["session"], scrape_date
        )
        if scraped_bbref_games_for_date:
            return get_bbref_games_for_date_from_s3(scrape_date)
        result = scrape_bbref_games_for_date(scrape_date, self.driver)
        if result.failure:
            return result
        bbref_games_for_date = result.value
        result = upload_bbref_games_for_date(bbref_games_for_date)
        if result.failure:
            return result
        result = update_bbref_games_for_date_single_date(
            self.db["session"], self.season, bbref_games_for_date
        )
        if result.failure:
            return result
        return Result.Ok(bbref_games_for_date)

    def get_brooks_games_for_date(self, scrape_date, bbref_games_for_date):
        scraped_brooks_games_for_date = DateScrapeStatus.verify_brooks_daily_dashboard_scraped_for_date(
            self.db["session"], scrape_date
        )
        if scraped_brooks_games_for_date:
            return Result.Ok()
        result = scrape_brooks_games_for_date(
            self.db["session"], self.driver, scrape_date, bbref_games_for_date
        )
        if result.failure:
            return result
        brooks_games_for_date = result.value
        result = upload_brooks_games_for_date(brooks_games_for_date)
        if result.failure:
            return result
        return update_brooks_games_for_date_single_date(
            self.db["session"], self.season, brooks_games_for_date, scrape_date
        )
