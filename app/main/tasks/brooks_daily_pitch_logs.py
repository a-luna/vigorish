import time
from random import randint

from tqdm import tqdm

from app.main.constants import PBAR_LEN_DICT
from app.main.models.status_date import DateScrapeStatus
from app.main.scrape.brooks.scrape_brooks_pitch_logs_for_date import scrape_brooks_pitch_logs_for_date
from app.main.status.update_status_brooks_pitch_logs import update_status_brooks_pitch_logs_for_game
from app.main.tasks.base_task import BaseTask
from app.main.util.dt_format_strings import DATE_ONLY
from app.main.util.s3_helper import get_brooks_games_for_date_from_s3, upload_brooks_pitch_logs_for_game
from app.main.util.result import Result


class ScrapeBrooksDailyPitchLogs(BaseTask):
    key_name = "brooks_pitch_log"
    display_name = "Pitch logs for date (brooksbaseball.com)"

    def execute(self, scrape_date):
        scraped_brooks_games_for_date = DateScrapeStatus.verify_brooks_daily_dashboard_scraped_for_date(
            self.db['session'], scrape_date)
        if not scraped_brooks_games_for_date:
            date_str = scrape_date.strftime(DATE_ONLY)
            error = (
                f"Brooks games for date {date_str} have not been scraped, unable to scrape Brooks "
                "pitch logs until this has been done.")
            return Result.Fail(error)
        result = get_brooks_games_for_date_from_s3(scrape_date)
        if result.failure:
            return result
        brooks_games_for_date = result.value
        result = scrape_brooks_pitch_logs_for_date(brooks_games_for_date)
        if result.failure:
            return result
        scraped_games = result.value
        for scraped_pitch_logs in scraped_games:
            result = update_status_brooks_pitch_logs_for_game(session, scraped_pitch_logs)
            if result.failure:
                return result
        with tqdm(total=len(scraped_games), unit="file", leave=False, position=2) as pbar:
            for scraped_pitch_logs in scraped_games:
                des = self.get_pbar_upload_description(scraped_pitch_logs.bbref_game_id)
                pbar.set_description(des)
                result = upload_brooks_pitch_logs_for_game(scraped_pitch_logs, scrape_date)
                if result.failure:
                    return result
                time.sleep(randint(50, 100) / 100.0)
                pbar.update()
        return Result.Ok()


    def get_pbar_upload_description(self, game_id):
        pre = f"Uploading | {game_id}"
        pad_len = PBAR_LEN_DICT[self.key_name] - len(pre)
        return f"{pre}{'.'*pad_len}"
