from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from tqdm import tqdm

from vigorish.config.database import DateScrapeStatus
from vigorish.enums import DataSet, ScrapeCondition, ScrapeTool
from vigorish.scrape.bbref_boxscores.parse_html import (
    _BATTING_STATS_TABLE,
    _PITCHING_STATS_TABLE,
    _PLAY_BY_PLAY_TABLE,
    parse_bbref_boxscore,
)
from vigorish.scrape.progress_bar import url_fetch_description, save_html_description
from vigorish.scrape.scrape_task import ScrapeTaskABC
from vigorish.status.update_status_bbref_boxscores import update_status_bbref_boxscore
from vigorish.util.decorators.retry import retry, RetryLimitExceededError
from vigorish.util.decorators.timeout import timeout
from vigorish.util.dt_format_strings import DATE_ONLY
from vigorish.util.result import Result

_BAT_STATS_TABLE_LOC = (By.XPATH, _BATTING_STATS_TABLE)
_PITCH_STATS_TABLE_LOC = (By.XPATH, _PITCHING_STATS_TABLE)
_PLAY_BY_PLAY_TABLE_LOC = (By.XPATH, _PLAY_BY_PLAY_TABLE)


class ScrapeBBRefBoxscores(ScrapeTaskABC):
    def __init__(self, db_job, db_session, config, scraped_data, driver, url_builder):
        self.data_set = DataSet.BBREF_BOXSCORES
        super().__init__(db_job, db_session, config, scraped_data, driver, url_builder)

    def check_prerequisites(self, game_date):
        scraped_bbref_games_for_date = DateScrapeStatus.verify_bbref_daily_dashboard_scraped_for_date(
            self.db_session, game_date
        )
        if scraped_bbref_games_for_date:
            return Result.Ok()
        date_str = game_date.strftime(DATE_ONLY)
        error = (
            f"BBref games for date {date_str} have not been scraped, unable to scrape BBref "
            "boxscores until this has been done."
        )
        return Result.Fail(error)

    def check_current_status(self, game_date):
        scraped_bbref_boxscores = DateScrapeStatus.verify_all_bbref_boxscores_scraped_for_date(
            self.db_session, game_date
        )
        if scraped_bbref_boxscores and self.scrape_condition == ScrapeCondition.ONLY_MISSING_DATA:
            return Result.Fail("skip")
        return Result.Ok()

    def scrape_html_with_requests_selenium(self, missing_html):
        with tqdm(total=len(missing_html), unit="url", leave=False, position=1) as pbar:
            for url_details in missing_html:
                url_id = url_details["identifier"]
                pbar.set_description(url_fetch_description(self.data_set, url_id))
                try:
                    html = render_webpage(driver, url_details["url"])
                    pbar.set_description(save_html_description(self.data_set, url_id))
                    result = self.save_scraped_html(html, url_id)
                    if result.failure:
                        return result
                    url_details["html_filepath"] = result.value
                    pbar.update()
                except RetryLimitExceededError as e:
                    return Result.Fail(repr(e))
                except Exception as e:
                    return Result.Fail(f"Error: {repr(e)}")

    @retry(max_attempts=3, delay=10, exceptions=(TimeoutError, Exception))
    @timeout(seconds=15)
    def render_webpage(self, url):
        self.driver.get(url)
        WebDriverWait(driver, 1000).until(ec.presence_of_element_located(_BAT_STATS_TABLE_LOC))
        WebDriverWait(driver, 1000).until(ec.presence_of_element_located(_PITCH_STATS_TABLE_LOC))
        WebDriverWait(driver, 1000).until(ec.presence_of_element_located(_PLAY_BY_PLAY_TABLE_LOC))
        return driver.page_source

    def parse_html(self, page_source, url):
        return parse_bbref_boxscore(page_source, url)

    def update_status(self, parsed_data):
        return update_status_bbref_boxscore(self.db_session, parsed_data)
