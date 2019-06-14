from app.main.models.status_date import DateScrapeStatus
from app.main.scrape.brooks.scrape_brooks_games_for_date import scrape_brooks_games_for_date
from app.main.status.update_status_brooks_games_for_date import update_brooks_games_for_date_single_date
from app.main.tasks.base_task import BaseTask
from app.main.util.dt_format_strings import DATE_ONLY_2
from app.main.util.result import Result
from app.main.util.s3_helper import get_bbref_games_for_date_from_s3, upload_brooks_games_for_date
from app.main.util.scrape_functions import get_chromedriver


class ScrapeBrooksDailyGames(BaseTask):
    key_name = "brooks_games_for_date"
    display_name = "Games for date (brooksbaseball.com)"

    def __init__(self):
        BaseTask.__init__(self)
        try:
            self.driver = get_chromedriver()
        except RetryLimitExceededError as e:
            self.driver = None
        except Exception as e:
            self.driver = None

    def execute(self, scrape_date):
        if not self.driver:
            return Result.Fail("Chromedriver was not instantiated, must abort.")
        scraped_bbref_games_for_date = DateScrapeStatus.verify_bbref_daily_dashboard_scraped_for_date(
            self.db['session'], scrape_date)
        if not scraped_bbref_games_for_date:
            date_str = scrape_date.strftime(DATE_ONLY_2)
            error = (
                f"BBref games for date ({date_str}) have not been scraped, unable to scrape Brooks "
                f"games for date ({date_str}) until this has been done.")
            return Result.Fail(error)
        result = get_bbref_games_for_date_from_s3(scrape_date)
        if result.failure:
            return result
        bbref_games_for_date = result.value
        result = scrape_brooks_games_for_date(
            self.db['session'], self.driver, scrape_date, bbref_games_for_date)
        self.driver.quit()
        self.driver = None
        if result.failure:
            return result
        brooks_games_for_date = result.value
        result = upload_brooks_games_for_date(brooks_games_for_date)
        if result.failure:
            return result
        return update_brooks_games_for_date_single_date(
            self.db['session'], self.season, brooks_games_for_date, scrape_date)
