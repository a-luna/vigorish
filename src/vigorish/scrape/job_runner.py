from collections import defaultdict
from datetime import datetime

from halo import Halo

from vigorish.config.database import ScrapeError
from vigorish.constants import EMOJI_DICT
from vigorish.enums import DataSet, JobStatus
from vigorish.scrape.bbref_boxscores.scrape_bbref_boxscores import ScrapeBBRefBoxscores
from vigorish.scrape.bbref_games_for_date.scrape_bbref_games_for_date import (
    ScrapeBBRefGamesForDate,
)
from vigorish.scrape.brooks_games_for_date.scrape_brooks_games_for_date import (
    ScrapeBrooksGamesForDate,
)
from vigorish.scrape.brooks_pitch_logs.scrape_brooks_pitch_logs import ScrapeBrooksPitchLogs
from vigorish.scrape.brooks_pitchfx.scrape_brooks_pitchfx import ScrapeBrooksPitchFx
from vigorish.scrape.util import get_chromedriver
from vigorish.util.decorators.retry import RetryLimitExceededError
from vigorish.util.result import Result

SCRAPE_TASK_DICT = {
    DataSet.BROOKS_GAMES_FOR_DATE: ScrapeBrooksGamesForDate,
    DataSet.BROOKS_PITCH_LOGS: ScrapeBrooksPitchLogs,
    DataSet.BROOKS_PITCHFX: ScrapeBrooksPitchFx,
    DataSet.BBREF_GAMES_FOR_DATE: ScrapeBBRefGamesForDate,
    DataSet.BBREF_BOXSCORES: ScrapeBBRefBoxscores,
}


class JobRunner:
    def __init__(self, db_job, db_session, config, scraped_data, url_builder):
        self.db_job = db_job
        self.data_sets = self.db_job.data_sets
        self.start_date = self.db_job.start_date
        self.end_date = self.db_job.end_date
        self.db_session = db_session
        self.config = config
        self.scraped_data = scraped_data
        self.url_builder = url_builder
        self.driver = None

    @property
    def duration(self):
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None

    @property
    def status_report(self):
        if self.db_job.status == JobStatus.COMPLETE:
            start_str = self.start_date.strftime(MONTH_NAME_SHORT)
            end_str = self.end_date.strftime(MONTH_NAME_SHORT)
            report = (
                "Requested data was successfully scraped:\n"
                f"data set.........: {self.data_sets}\n"
                f"date range.......: {start_str} - {end_str}\n"
                f"duration.........: {format_timedelta_str(self.duration)}"
            )
        elif self.db_job.status == JobStatus.ERROR:
            return str(self.result)
        else:
            return self.status

    def execute(self):
        result = self.initialize()
        if result.failure:
            return self.job_failed(result)
        spinners = defaultdict(lambda: Halo(spinner="weather"))
        for i, data_set in enumerate(self.data_sets, start=1):
            text = f"Scraping data set: {data_set.name} (Task #{i}/{len(self.data_sets)})..."
            spinners[data_set].text = text
            spinners[data_set].start()
            scrape_task = SCRAPE_TASK_DICT[data_set](
                self.db_job,
                self.db_session,
                self.config,
                self.scraped_data,
                self.driver,
                self.url_builder,
            )
            spinners[data_set].stop_and_persist(spinners[data_set].frame(), "")
            result = scrape_task.execute(scrape_date)
            if result.failure:
                if "skip" in result.error:
                    text = f"Skipped data set: {data_set.name} ((Task #{i}/{len(self.data_sets)}))"
                    spinners[data_set].stop_and_persist(EMOJI_DICT.get("SHRUG", ""), text)
                    continue
                test = f"Failed to scrape: {data_set.name} ((Task #{i}/{len(self.data_sets)}))"
                spinners[data_set].stop_and_persist(EMOJI_DICT.get("WEARY", ""), text)
                return self.job_failed(result)
            text = f"Scraped data set: {data_set.name} ((Task #{i}/{len(self.data_sets)}))"
            spinners[data_set].stop_and_persist(EMOJI_DICT.get("THUMBS_UP", ""), text)
        return self.job_succeeded()

    def initialize(self):
        try:
            self.db_job.status = JobStatus.PREPARING
            self.db_session.commit()
            self.driver = get_chromedriver()
            self.start_time = datetime.now()
            return Result.Ok()
        except RetryLimitExceededError as e:
            return Result.Fail(repr(e))
        except Exception as e:
            return Result.Fail(f"Error: {repr(e)}")

    def job_failed(self, result, data_set=DataSet.ALL):
        self.db_job.status = JobStatus.ERROR
        new_error = ScrapeError(
            error_message=result.error, data_set=data_set, job_id=self.db_job.id
        )
        self.db_session.add(new_error)
        self.db_session.commit()
        self._tear_down()
        return result

    def job_succeeded(self):
        self.db_job.status = JobStatus.COMPLETE
        self.db_session.commit()
        self._tear_down()
        return Result.Ok()

    def tear_down(self):
        self.end_time = datetime.now()
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
            except Exception:
                return Result.Fail(f"Error occurred quitting chromedriver: {repr(e)}")
