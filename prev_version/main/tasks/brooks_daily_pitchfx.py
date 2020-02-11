import time
from random import randint

from tqdm import tqdm

from app.main.constants import PBAR_LEN_DICT
from app.main.models.status_date import DateScrapeStatus
from app.main.models.status_pitch_appearance import PitchAppearanceScrapeStatus
from app.main.scrape.brooks.scrape_brooks_pitchfx import (
    scrape_brooks_pitchfx_logs_for_game,
)
from app.main.status.update_status_brooks_pitchfx import (
    update_pitch_appearance_status_records,
)
from app.main.tasks.base_task import BaseTask
from app.main.util.dt_format_strings import DATE_ONLY_2
from app.main.util.result import Result
from app.main.util.s3_helper import (
    get_all_brooks_pitch_logs_for_date_from_s3,
    upload_brooks_pitchfx_log,
)


class ScrapeBrooksDailyPitchFxLogs(BaseTask):
    key_name = "brooks_pitchfx"
    display_name = "PitchFX for pitching appearance (brooksbaseball.com)"

    def execute(self, scrape_date):
        scraped_brooks_pitch_logs = DateScrapeStatus.verify_all_brooks_pitch_logs_scraped_for_date(
            self.db["session"], scrape_date
        )
        if not scraped_brooks_pitch_logs:
            date_str = scrape_date.strftime(DATE_ONLY_2)
            error = (
                f"Brooks pitch logs for date {date_str} have not been scraped, unable to scrape Brooks "
                "pitchfx data until this has been done."
            )
            return Result.Fail(error)
        scraped_brooks_pitchfx = DateScrapeStatus.verify_all_brooks_pitchfx_scraped_for_date(
            self.db["session"], scrape_date
        )
        if scraped_brooks_pitchfx:
            return Result.Ok("skipped")
        result = get_all_brooks_pitch_logs_for_date_from_s3(
            self.db["session"], scrape_date
        )
        if result.failure:
            return result
        pitch_logs_for_date = result.value
        scraped_count = 0
        scraped_pitchfx_logs = {}
        scrape_audit = []
        with tqdm(
            total=len(pitch_logs_for_date), unit="game", leave=False, position=2
        ) as pbar:
            for pitch_logs_for_game in pitch_logs_for_date:
                bbref_game_id = pitch_logs_for_game.bbref_game_id
                pbar.set_description(self.get_pbar_game_id_description(bbref_game_id))
                scraped_pitch_app_ids = PitchAppearanceScrapeStatus.get_all_scraped_pitch_app_ids_for_game(
                    self.db["session"], bbref_game_id
                )
                result = scrape_brooks_pitchfx_logs_for_game(
                    pitch_logs_for_game, scraped_pitch_app_ids
                )
                if result.failure:
                    return result
                (pitchfx_logs_for_game, scrape_audit_for_game) = result.value
                scraped_count += len(pitchfx_logs_for_game)
                scraped_pitchfx_logs[bbref_game_id] = pitchfx_logs_for_game
                scrape_audit.append(scrape_audit_for_game)
                pbar.update()
        with tqdm(total=scraped_count, unit="file", leave=False, position=2) as pbar:
            for bbref_game_id, pitchfx_logs_for_game in scraped_pitchfx_logs.items():
                pbar.set_description(self.get_pbar_uploading_description(bbref_game_id))
                for pitchfx_log in pitchfx_logs_for_game:
                    result = upload_brooks_pitchfx_log(pitchfx_log)
                    if result.failure:
                        return result
                    pbar.update()
        with tqdm(
            total=scraped_count, unit="pitch_log", leave=False, position=2
        ) as pbar:
            for bbref_game_id, pitchfx_logs_for_game in scraped_pitchfx_logs.items():
                pbar.set_description(self.get_pbar_updating_description(bbref_game_id))
                for pitchfx_log in pitchfx_logs_for_game:
                    result = update_pitch_appearance_status_records(
                        self.db["session"], pitchfx_log
                    )
                    if result.failure:
                        return result
                    pbar.update()
        return Result.Ok(scrape_audit)

    def get_pbar_game_id_description(self, game_id):
        pre = f"Game ID   | {game_id}"
        pad_len = PBAR_LEN_DICT[self.key_name] - len(pre)
        return f"{pre}{'.'*pad_len}"

    def get_pbar_uploading_description(self, game_id):
        pre = f"Uploading | {game_id}"
        pad_len = PBAR_LEN_DICT[self.key_name] - len(pre)
        return f"{pre}{'.'*pad_len}"

    def get_pbar_updating_description(self, game_id):
        pre = f"Updating  | {game_id}"
        pad_len = PBAR_LEN_DICT[self.key_name] - len(pre)
        return f"{pre}{'.'*pad_len}"