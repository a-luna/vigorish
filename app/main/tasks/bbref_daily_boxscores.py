import time
from random import randint

from tqdm import tqdm

from app.main.constants import PBAR_LEN_DICT
from app.main.models.status_date import DateScrapeStatus
from app.main.scrape.bbref.scrape_bbref_boxscores_for_date import scrape_bbref_boxscores_for_date
from app.main.status.update_status_bbref_boxscores import update_status_bbref_boxscore
from app.main.tasks.base_task import BaseTask
from app.main.util.dt_format_strings import DATE_ONLY
from app.main.util.s3_helper import get_bbref_games_for_date_from_s3, upload_bbref_boxscore
from app.main.util.result import Result


class ScrapeBBRefDailyBoxscores(BaseTask):
    key_name = "bbref_boxscore"
    display_name = "Boxscores for date (bbref.com)"

    def execute(self, scrape_date):
        scraped_bbref_games_for_date = DateScrapeStatus.verify_bbref_daily_dashboard_scraped_for_date(
            self.db['session'], scrape_date)
        if not scraped_bbref_games_for_date:
            date_str = scrape_date.strftime(DATE_ONLY)
            error = (
                f"BBref games for date {date_str} have not been scraped, unable to scrape BBref "
                "boxscores until this has been done.")
            return Result.Fail(error)
        #scraped_bbref_boxscores = DateScrapeStatus.verify_all_bbref_boxscores_scraped_for_date(
        #    self.db['session'], scrape_date)
        #if scraped_bbref_boxscores:
        #    return Result.Ok("skipped")
        result = get_bbref_games_for_date_from_s3(scrape_date)
        if result.failure:
            return result
        bbref_games_for_date = result.value
        result = scrape_bbref_boxscores_for_date(bbref_games_for_date, self.driver)
        if result.failure:
            return result
        (scraped_games, scrape_audit) = result.value
        with tqdm(total=len(scraped_games), unit="file", leave=False, position=2) as pbar:
            for scraped_boxscore in scraped_games:
                pbar.set_description(self.get_pbar_upload_description(scraped_boxscore.bbref_game_id))
                result = upload_bbref_boxscore(scraped_boxscore)
                if result.failure:
                    return result
                time.sleep(randint(25, 50) / 100.0)
                pbar.update()
        with tqdm(total=len(scraped_games), unit="file", leave=False, position=2) as pbar:
            for scraped_boxscore in scraped_games:
                pbar.set_description(self.get_pbar_updating_description(scraped_boxscore.bbref_game_id))
                result = update_status_bbref_boxscore(self.db['session'], scraped_boxscore)
                if result.failure:
                    return result
                time.sleep(randint(10, 30) / 100.0)
                pbar.update()
        return Result.Ok(scrape_audit)


    def get_pbar_updating_description(self, game_id):
        pre = f"Updating  | {game_id}"
        pad_len = PBAR_LEN_DICT[self.key_name] - len(pre)
        return f"{pre}{'.'*pad_len}"


    def get_pbar_upload_description(self, game_id):
        pre = f"Uploading | {game_id}"
        pad_len = PBAR_LEN_DICT[self.key_name] - len(pre)
        return f"{pre}{'.'*pad_len}"
