from vigorish.config.database import DateScrapeStatus
from vigorish.enums import DataSet, ScrapeCondition
from vigorish.scrape.brooks_pitchfx.parse_html import parse_pitchfx_log
from vigorish.scrape.scrape_task import ScrapeTaskABC
from vigorish.status.update_status_brooks_pitchfx import update_pitch_appearance_status_records
from vigorish.util.dt_format_strings import DATE_ONLY_2
from vigorish.util.result import Result


class ScrapeBrooksPitchFx(ScrapeTaskABC):
    def __init__(self, db_job, db_session, config, scraped_data):
        self.data_set = DataSet.BROOKS_PITCHFX
        super().__init__(db_job, db_session, config, scraped_data)

    def check_prerequisites(self, game_date):
        brooks_pitch_logs = DateScrapeStatus.verify_all_brooks_pitch_logs_scraped_for_date(
            self.db_session, game_date
        )
        if brooks_pitch_logs:
            return Result.Ok()
        date_str = game_date.strftime(DATE_ONLY_2)
        error = (
            f"Brooks pitch logs for date {date_str} have not been scraped, unable to scrape "
            "Brooks pitchfx data until this has been done."
        )
        return Result.Fail(error)

    def check_current_status(self, game_date):
        scraped_brooks_pitchfx = DateScrapeStatus.verify_all_brooks_pitchfx_scraped_for_date(
            self.db_session, game_date
        )
        if scraped_brooks_pitchfx and self.scrape_condition == ScrapeCondition.ONLY_MISSING_DATA:
            return Result.Fail("skip")
        return Result.Ok()

    def parse_scraped_html(self):
        parsed = 0
        for game_date in self.date_range:
            pitch_logs_for_date = self.scraped_data.get_all_brooks_pitch_logs_for_date(game_date)
            if not pitch_logs_for_date:
                date_str = game_date.strftime(DATE_ONLY_2)
                error = f"Failed to retrieve {DataSet.BROOKS_PITCH_LOGS} for date: {date_str}"
                return Result.Fail(error)
            for pitch_logs_for_game in pitch_logs_for_date:
                game_id = pitch_logs_for_game.bbref_game_id
                self.spinner.text = self.url_tracker.parse_html_report(parsed, game_id)
                for pitch_log in pitch_logs_for_game.pitch_logs:
                    if not pitch_log.parsed_all_info:
                        continue
                    if pitch_log.pitch_app_id not in self.url_tracker.parse_url_ids:
                        continue
                    html = self.url_tracker.get_html(pitch_log.pitch_app_id)
                    result = parse_pitchfx_log(html, pitch_log)
                    if result.failure:
                        return result
                    pitchfx_log = result.value
                    result = self.scraped_data.save_json(self.data_set, pitchfx_log)
                    if result.failure:
                        return Result.Fail(f"Error! {result.error} (ID: {pitch_log.pitch_app_id})")
                    result = self.update_status(game_date, pitchfx_log)
                    if result.failure:
                        return Result.Fail(f"Error! {result.error} (ID: {pitch_log.pitch_app_id})")
                    parsed += 1
                    self.spinner.text = self.url_tracker.parse_html_report(parsed, game_id)
                    self.db_session.commit()
        return Result.Ok()

    def parse_html(self, html, url_id, url):
        pass

    def update_status(self, game_date, parsed_data):
        return update_pitch_appearance_status_records(self.db_session, parsed_data)
