import time
from random import randint

from tqdm import tqdm

from app.main.constants import PBAR_LEN_DICT
from app.main.models.status_date import DateScrapeStatus
from app.main.scrape.brooks.scrape_brooks_pitch_logs_for_date import scrape_brooks_pitch_logs_for_date
from app.main.status.update_status_brooks_pitch_logs import update_status_brooks_pitch_logs_for_game
from app.main.tasks.base_task import BaseTask
from app.main.util.dt_format_strings import DATE_ONLY_2
from app.main.util.s3_helper import (
    get_brooks_games_for_date_from_s3,
    get_all_brooks_pitch_logs_for_date_from_s3,
    upload_brooks_pitch_logs_for_game,
)
from app.main.util.result import Result


class ScrapeBrooksDailyPitchLogs(BaseTask):
    key_name = "brooks_pitch_logs"
    display_name = "Pitch logs for date (brooksbaseball.com)"

    def execute(self, scrape_date):
        scraped_brooks_games_for_date = DateScrapeStatus.verify_brooks_daily_dashboard_scraped_for_date(
            self.db['session'], scrape_date)
        if not scraped_brooks_games_for_date:
            date_str = scrape_date.strftime(DATE_ONLY_2)
            error = (
                f"Brooks games for date {date_str} have not been scraped, unable to scrape Brooks "
                "pitch logs until this has been done.")
            return Result.Fail(error)
        scraped_brooks_pitch_logs = DateScrapeStatus.verify_all_brooks_pitch_logs_scraped_for_date(
            self.db['session'], scrape_date)
        if scraped_brooks_pitch_logs:
            return Result.Ok("skipped")
        result = get_brooks_games_for_date_from_s3(scrape_date)
        if result.failure:
            return result
        brooks_games_for_date = result.value
        result = get_all_brooks_pitch_logs_for_date_from_s3(self.db['session'], scrape_date)
        if result.failure:
            return result
        brooks_pitch_logs_for_date = result.value
        result = scrape_brooks_pitch_logs_for_date(brooks_pitch_logs_for_date, brooks_games_for_date)
        if result.failure:
            return result
        scraped_games = result.value
        with tqdm(total=len(scraped_games), unit="file", leave=False, position=2) as pbar:
            for scraped_pitch_logs in scraped_games:
                pbar.set_description(self.get_pbar_upload_description(scraped_pitch_logs.bbref_game_id))
                result = upload_brooks_pitch_logs_for_game(scraped_pitch_logs)
                if result.failure:
                    return result
                time.sleep(randint(25, 50) / 100.0)
                pbar.update()
        with tqdm(total=len(scraped_games), unit="game", leave=False, position=2) as pbar:
            for scraped_pitch_logs in scraped_games:
                pbar.set_description(self.get_pbar_updating_description(scraped_pitch_logs.bbref_game_id))
                result = update_status_brooks_pitch_logs_for_game(self.db['session'], scraped_pitch_logs, scrape_date)
                if result.failure:
                    return result
                time.sleep(randint(10, 30) / 100.0)
                pbar.update()
        return Result.Ok("scraped_brooks")


    def get_pbar_updating_description(self, game_id):
        pre = f"Updating  | {game_id}"
        pad_len = PBAR_LEN_DICT[self.key_name] - len(pre)
        return f"{pre}{'.'*pad_len}"


    def get_pbar_upload_description(self, game_id):
        pre = f"Uploading | {game_id}"
        pad_len = PBAR_LEN_DICT[self.key_name] - len(pre)
        return f"{pre}{'.'*pad_len}"
