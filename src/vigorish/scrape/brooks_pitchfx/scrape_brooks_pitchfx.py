from halo import Halo
from lxml import html

from vigorish.config.database import DateScrapeStatus
from vigorish.constants import JOB_SPINNER_COLORS
from vigorish.enums import DataSet, ScrapeCondition, PythonScrapeTool, JobStatus
from vigorish.scrape.scrape_task import ScrapeTaskABC
from vigorish.scrape.brooks_pitchfx.parse_html import parse_pitchfx_log
from vigorish.status.update_status_brooks_pitchfx import update_pitch_appearance_status_records
from vigorish.util.dt_format_strings import DATE_ONLY_2
from vigorish.util.result import Result


class ScrapeBrooksPitchFx(ScrapeTaskABC):
    def __init__(self, db_job, db_session, config, scraped_data, driver):
        self.data_set = DataSet.BROOKS_PITCHFX
        super().__init__(db_job, db_session, config, scraped_data, driver)

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

    def scrape_html_with_requests_selenium(self, missing_html):
        self.python_scrape_tool = PythonScrapeTool.REQUESTS
        super().scrape_html_with_requests_selenium(missing_html)

    def parse_data_from_scraped_html(self):
        self.db_job.status = JobStatus.PARSING
        self.db_session.commit()
        spinner = Halo(color=JOB_SPINNER_COLORS[self.data_set], spinner="dots3")
        spinner.text = "Parsing HTML..."
        spinner.start()
        parsed = 0
        for game_date in self.date_range:
            result = self.scraped_data.get_all_brooks_pitch_logs_for_date(game_date)
            if result.failure:
                spinner.fail(f"Error! {result.error}")
                return result
            pitch_logs_for_date = result.value
            for pitch_logs_for_game in pitch_logs_for_date:
                game_id = pitch_logs_for_game.bbref_game_id
                spinner.text = (
                    f"Parsing PitchFX Logs for {game_id}... "
                    f"{parsed / float(self.total_urls):.0%} ({parsed}/{self.total_urls} URLs)"
                )
                for pitch_log in pitch_logs_for_game.pitch_logs:
                    pitchfx_url = pitch_log.pitchfx_url
                    if not pitch_log.parsed_all_info:
                        continue
                    filepath = self.scraped_data.get_html(self.data_set, pitch_log.pitch_app_id)
                    if not filepath:
                        error = f"Failed to retrieve HTML for pitch app: {pitch_log.pitch_app_id}"
                        spinner.fail(error)
                        return Result.Fail(error)
                    page_source = html.fromstring(filepath.read_text(), base_url=pitchfx_url)
                    result = parse_pitchfx_log(page_source, pitch_log)
                    if result.failure:
                        return result
                    pitchfx_log = result.value
                    result = self.scraped_data.save_json(self.data_set, pitchfx_log)
                    if result.failure:
                        spinner.fail(f"Error! {result.error} (ID: {pitch_log.pitch_app_id})")
                        return result
                    result = self.update_status(game_date, pitchfx_log)
                    if result.failure:
                        spinner.fail(f"Error! {result.error} (ID: {pitch_log.pitch_app_id})")
                        return result
                    parsed += 1
                    spinner.text = (
                        f"Parsing PitchFX Logs for {game_id}... "
                        f"{parsed / float(self.total_urls):.0%} ({parsed}/{self.total_urls} URLs)"
                    )
        spinner.succeed("HTML Parsed")
        return Result.Ok()

    def parse_html(self, page_source, url_id, url):
        pass

    def update_status(self, game_date, parsed_data):
        return update_pitch_appearance_status_records(self.db_session, parsed_data)
