import shutil
import subprocess
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from halo import Halo
from getch import pause

from vigorish.cli.util import print_message
from vigorish.config.database import ScrapeError
from vigorish.constants import EMOJI_DICT, JOB_SPINNER_COLORS
from vigorish.enums import DataSet, StatusReport, JobStatus
from vigorish.status.report_status import report_season_status, report_date_range_status
from vigorish.util.result import Result

APP_FOLDER = Path(__file__).parent.parent
NODEJS_OUTBOX = APP_FOLDER.joinpath("nightmarejs/outbox")
JOB_STATUS_TEXT_COLOR = {"scraped": "bright_green", "skipped": "blue"}


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
        self.status_report = self.config.get_current_setting("STATUS_REPORT", DataSet.ALL)

    def execute(self):
        result = self.initialize()
        if result.failure:
            return self.job_failed(result)
        task_results = []
        spinners = defaultdict(lambda: Halo(spinner="dots3"))
        for i, data_set in enumerate(self.data_sets, start=1):
            subprocess.run(["clear"])
            if task_results:
                print_message("\n".join(task_results), fg="gray")
            text = f"Scraping data set: {data_set.name} (Task {i}/{len(self.data_sets)})..."
            spinners[data_set].text = text
            spinners[data_set].color = JOB_SPINNER_COLORS[data_set]
            spinners[data_set].start()
            spinners[data_set].stop_and_persist(spinners[data_set].frame(), "")
            result = self.get_scrape_task_for_data_set(data_set).execute()
            if result.failure:
                if "skip" in result.error:
                    task_results.append(self.scrape_task_skipped_text(data_set, i))
                    continue
                return self.job_failed(result, data_set=data_set)
            task_results.append(self.scrape_task_complete_text(data_set, i))
        subprocess.run(["clear"])
        print("\n".join(task_results))
        result = self.show_status_report()
        pause(message="Press any key to continue...")
        if result.failure:
            return self.job_failed(result)
        return self.job_succeeded()

    def get_scrape_task_for_data_set(self, data_set):
        task_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: ScrapeBrooksGamesForDate,
            DataSet.BROOKS_PITCH_LOGS: ScrapeBrooksPitchLogs,
            DataSet.BROOKS_PITCHFX: ScrapeBrooksPitchFx,
            DataSet.BBREF_GAMES_FOR_DATE: ScrapeBBRefGamesForDate,
            DataSet.BBREF_BOXSCORES: ScrapeBBRefBoxscores,
        }
        return task_dict[data_set](self.db_job, self.db_session, self.config, self.scraped_data)

    def scrape_task_skipped_text(self, data_set, task_number):
        return (
            f"{EMOJI_DICT.get('SHRUG', '')} Skipped data set: {data_set.name} "
            f"(Task {task_number}/{len(self.data_sets)})"
        )

    def scrape_task_complete_text(self, data_set, task_number):
        return (
            f"{EMOJI_DICT.get('PASSED', '')} Scraped data set: {data_set.name} "
            f"(Task {task_number}/{len(self.data_sets)})"
        )

    def initialize(self):
        errors = []
        self.start_time = datetime.now()
        result = self.check_url_delay_settings()
        if result.failure:
            errors.append(result.error)
        result = self.check_s3_bucket() if self.s3_bucket_required() else Result.Ok()
        if result.failure:
            errors.append(result.error)
        result = self.create_all_folderpaths()
        if result.failure:
            errors.append(result.error)
        if not errors:
            return Result.Ok()
        return Result.Fail("\n".join(errors))

    def show_status_report(self):
        if self.status_report == StatusReport.SEASON_SUMMARY:
            return report_season_status(
                session=self.db_session,
                scraped_data=self.scraped_data,
                refresh_data=False,
                year=self.season.year,
                report_type=self.status_report,
            )
        return report_date_range_status(
            session=self.db_session,
            scraped_data=self.scraped_data,
            refresh_data=False,
            start_date=self.db_job.start_date,
            end_date=self.db_job.end_date,
            report_type=self.status_report,
        )

    def job_failed(self, result, data_set=DataSet.ALL):
        self.end_time = datetime.now()
        self.db_job.status = JobStatus.ERROR
        new_error = ScrapeError(
            error_message=result.error, data_set=data_set, job_id=self.db_job.id
        )
        self.db_session.add(new_error)
        self.db_session.commit()
        return result

    def job_succeeded(self):
        self.end_time = datetime.now()
        self.db_job.status = JobStatus.COMPLETE
        self.db_session.commit()
        shutil.rmtree(NODEJS_OUTBOX.joinpath(self.db_job.id))
        return Result.Ok()

    def create_all_folderpaths(self):
        return self.scraped_data.create_all_folderpaths(self.season.year)

    def check_url_delay_settings(self):
        return self.config.check_url_delay_settings(self.db_job.data_sets)

    def s3_bucket_required(self):
        return self.config.s3_bucket_required(self.db_job.data_sets)

    def check_s3_bucket(self):
        return self.scraped_data.check_s3_bucket(self.db_job.data_sets)
