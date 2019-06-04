import time
from random import randint

from tqdm import tqdm

from app.main.constants import PBAR_LEN_DICT
from app.main.models.status_date import DateScrapeStatus
from app.main.scrape.brooks.scrape_brooks_pitchfx import scrape_brooks_pitchfx_logs_for_game
from app.main.tasks.base_task import BaseTask
from app.main.util.dt_format_strings import DATE_ONLY_2
from app.main.util.result import Result
from app.main.util.s3_helper import get_brooks_pitch_logs_for_game_from_s3, upload_brooks_pitchfx_log


class ScrapeBrooksDailyPitchFxLogs(BaseTask):
    key_name = "brooks_pitchfx"
    display_name = "PitchFX for pitching appearance (brooksbaseball.com)"

    def execute(self, scrape_date):
        #all_pitch_logs_scraped = DateScrapeStatus.verify_all_brooks_pitch_logs_scraped_for_date(
        #    self.db['session'], scrape_date)
        #if not all_pitch_logs_scraped:
        #    error = f"Brooks pitch logs HAVE NOT been scraped for date: {scrape_date.strftime(DATE_ONLY_2)}"
        #    return Result.Fail(error)
        brooks_game_ids = DateScrapeStatus.get_all_brooks_game_ids_for_date(self.db['session'], scrape_date)
        with tqdm(total=len(brooks_game_ids), unit="game", leave=False, position=2) as pbar_game_id:
            for brooks_game_id in brooks_game_ids:
                pbar_game_id.set_description(self.get_pbar_game_id_description(brooks_game_id))
                result = get_brooks_pitch_logs_for_game_from_s3(brooks_game_id)
                if result.failure:
                    return result
                pitch_logs_for_game = result.value
                result = scrape_brooks_pitchfx_logs_for_game(pitch_logs_for_game)
                if result.failure:
                    return result
                pitchfx_logs_for_game = result.value
                with tqdm(total=len(pitchfx_logs_for_game), unit="file", leave=False, position=3) as pbar_uploading:
                    for pitchfx_log in pitchfx_logs_for_game:
                        pbar_uploading.set_description(
                            self.get_pbar_uploading_description(pitchfx_log.pitcher_id_mlb))
                        result = upload_brooks_pitchfx_log(pitchfx_log)
                        if result.failure:
                            return result
                        time.sleep(randint(25, 75) / 100.0)
                        pbar_uploading.update()
                #with tqdm(total=len(pitchfx_logs_for_game), unit="pitchfx", leave=False, position=3) as pbar_updating:
                #    for pitchfx_log in pitchfx_logs_for_game:
                #        pbar_updating.set_description(
                #            self.get_pbar_updating_description(pitchfx_log.pitcher_id_mlb))
                #        time.sleep(randint(25, 75) / 100.0)
                #        pbar_uploading.update()
                #pbar_game_id.update()
        return Result.Ok()



    def get_pbar_game_id_description(game_id):
        pre =f"Game ID   | {game_id}"
        pad_len = PBAR_LEN_DICT[self.key_name] - len(pre)
        return f"{pre}{'.'*pad_len}"


    def get_pbar_uploading_description(player_id):
        pre =f"Uploading | {player_id}"
        pad_len = PBAR_LEN_DICT[self.key_name] - len(pre)
        return f"{pre}{'.'*pad_len}"


    def get_pbar_updating_description(player_id):
        pre =f"Updating | {player_id}"
        pad_len = PBAR_LEN_DICT[self.key_name] - len(pre)
        return f"{pre}{'.'*pad_len}"
