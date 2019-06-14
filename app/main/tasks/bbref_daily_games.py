from app.main.scrape.bbref.scrape_bbref_games_for_date import scrape_bbref_games_for_date
from app.main.status.update_status_bbref_games_for_date import update_bbref_games_for_date_single_date
from app.main.tasks.base_task import BaseTask
from app.main.util.s3_helper import upload_bbref_games_for_date
from app.main.util.scrape_functions import get_chromedriver

class ScrapeBBRefDailyGames(BaseTask):
    key_name = "bbref_games_for_date"
    display_name = "Games for date (bbref.com)"

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
        result = scrape_bbref_games_for_date(scrape_date, self.driver)
        self.driver.quit()
        self.driver = None
        if result.failure:
            return result
        bbref_games_for_date = result.value
        result = upload_bbref_games_for_date(bbref_games_for_date)
        if result.failure:
            return result
        return update_bbref_games_for_date_single_date(self.db['session'], self.season, bbref_games_for_date)
