import subprocess
from collections import defaultdict
from datetime import datetime

from halo import Halo
from getch import pause

from vigorish.config.database import ScrapeError
from vigorish.constants import EMOJI_DICT, JOB_SPINNER_COLORS
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
from vigorish.status.report_status import report_season_status
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
    def __init__(self, db_job, db_session, config, scraped_data):
        self.db_job = db_job
        self.data_sets = self.db_job.data_sets
        self.start_date = self.db_job.start_date
        self.end_date = self.db_job.end_date
        self.season = self.db_job.season
        self.db_session = db_session
        self.config = config
        self.scraped_data = scraped_data
        self.driver = None
        self.status_report = self.config.get_current_setting("STATUS_REPORT", DataSet.ALL)

    def execute(self):
        if self.db_job.status == JobStatus.COMPLETE:
            error = "This job cannot be started, status is COMPLETE."
            return Result.Fail(error)
        result = self.initialize()
        if result.failure:
            return self.job_failed(result)
        task_results = []
        spinners = defaultdict(lambda: Halo(spinner="dots3"))
        for i, data_set in enumerate(self.data_sets, start=1):
            subprocess.run(["clear"])
            if task_results:
                print("\n".join(task_results))
            text = f"Scraping data set: {data_set.name} (Task #{i}/{len(self.data_sets)})..."
            spinners[data_set].text = text
            spinners[data_set].color = JOB_SPINNER_COLORS[data_set]
            spinners[data_set].start()
            scrape_task = SCRAPE_TASK_DICT[data_set](
                self.db_job, self.db_session, self.config, self.scraped_data, self.driver,
            )
            spinners[data_set].stop_and_persist(spinners[data_set].frame(), "")
            result = scrape_task.execute()
            if result.failure:
                if "skip" in result.error:
                    text = (
                        f"{EMOJI_DICT.get('SHRUG', '')} "
                        f"Skipped data set: {data_set.name} (Task #{i}/{len(self.data_sets)})"
                    )
                    task_results.append(text)
                    continue
                return self.job_failed(result)
            text = (
                f"{EMOJI_DICT.get('PASSED', '')} "
                f"Scraped data set: {data_set.name} (Task #{i}/{len(self.data_sets)})"
            )
            task_results.append(text)
        subprocess.run(["clear"])
        print("\n".join(task_results))
        pause(message="Press any key to continue...")
        subprocess.run(["clear"])
        result = report_season_status(
            session=self.db_session,
            scraped_data=self.scraped_data,
            refresh_data=True,
            year=self.season.year,
            report_type=self.status_report,
        )
        if result.failure:
            return self.job_failed(result)
        return self.job_succeeded()

    def initialize(self):
        self.start_time = datetime.now()
        result = self.scraped_data.create_all_folderpaths(self.season.year)
        if result.failure:
            return result
        try:
            self.driver = get_chromedriver() if self.config.selenium_required() else None
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
        self.tear_down()
        return result

    def job_succeeded(self):
        self.db_job.status = JobStatus.COMPLETE
        self.db_session.commit()
        self.tear_down()
        return Result.Ok()

    def tear_down(self):
        self.end_time = datetime.now()
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
            except Exception as e:
                return Result.Fail(f"Error occurred quitting chromedriver: {repr(e)}")
