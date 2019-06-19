from app.main.scrape.bbref.scrape_bbref_games_for_date import scrape_bbref_games_for_date
from app.main.scrape.brooks.scrape_brooks_games_for_date import scrape_brooks_games_for_date
from app.main.status.update_status_bbref_games_for_date import update_bbref_games_for_date_single_date
from app.main.status.update_status_brooks_games_for_date import update_brooks_games_for_date_single_date
from app.main.tasks.base_task import BaseTask
from app.main.util.s3_helper import upload_bbref_games_for_date, upload_brooks_games_for_date


class FindAllGamesForDateTask(BaseTask):
    key_name = "find_games_for_date"
    display_name = "Games for date (bbref.com)"

    def execute(self, scrape_date):
        print("STARTED: Scrape BBref")
        result = scrape_bbref_games_for_date(scrape_date)
        if result.failure:
            return result
        bbref_games_for_date = result.value
        print("FINISHED: Scrape BBref")
        print("STARTED: Upload BBref")
        result = upload_bbref_games_for_date(bbref_games_for_date)
        if result.failure:
            return result
        print("FINISHED: Upload BBref")
        print("STARTED: Update BBref")
        result =  update_bbref_games_for_date_single_date(
            self.db['session'], self.season, bbref_games_for_date)
        if result.failure:
            return result
        print("FINISHED: Update BBref")
        print("STARTED: Scrape Brooks")
        result = scrape_brooks_games_for_date(
            self.db['session'], scrape_date, bbref_games_for_date)
        if result.failure:
            return result
        brooks_games_for_date = result.value
        print("FINISHED: Scrape Brooks")
        print("STARTED: Upload Brooks")
        result = upload_brooks_games_for_date(brooks_games_for_date)
        if result.failure:
            return result
        print("FINISHED: Upload Brooks")
        print("STARTED: Update Brooks")
        result = update_brooks_games_for_date_single_date(
            self.db['session'], self.season, brooks_games_for_date, scrape_date)
        if result.failure:
            return result
        print("FINISHED: Update Brooks")
        return Result.Ok()
