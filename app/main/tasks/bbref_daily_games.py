from app.main.models.status_date import DateScrapeStatus
from app.main.scrape.bbref.scrape_bbref_games_for_date import scrape_bbref_games_for_date
from app.main.status.update_status_bbref_games_for_date import update_bbref_games_for_date_single_date
from app.main.tasks.base_task import BaseTask
from app.main.util.s3_helper import upload_bbref_games_for_date
from app.main.util.result import Result

class ScrapeBBRefDailyGames(BaseTask):
    key_name = "bbref_games_for_date"
    display_name = "Games for date (bbref.com)"

    def execute(self, scrape_date):
        scraped_bbref_games_for_date = DateScrapeStatus.verify_bbref_daily_dashboard_scraped_for_date(
            self.db['session'], scrape_date)
        if scraped_bbref_games_for_date:
            return Result.Ok("skipped")
        result = scrape_bbref_games_for_date(scrape_date, self.driver)
        if result.failure:
            return result
        bbref_games_for_date = result.value
        result = upload_bbref_games_for_date(bbref_games_for_date)
        if result.failure:
            return result
        result = update_bbref_games_for_date_single_date(self.db['session'], self.season, bbref_games_for_date)
        if result.failure:
            return result
        return Result.Ok("scraped_brooks")
