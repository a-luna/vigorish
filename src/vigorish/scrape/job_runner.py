import shutil
import subprocess
from collections import defaultdict
from datetime import datetime

from getch import pause
from halo import Halo

from vigorish.cli.components import print_message
from vigorish.config.database import ScrapeError
from vigorish.constants import FAKE_SPINNER, JOB_SPINNER_COLORS
from vigorish.enums import DataSet, JobStatus, StatusReport
from vigorish.scrape.bbref_boxscores.scrape_task import ScrapeBBRefBoxscores
from vigorish.scrape.bbref_games_for_date.scrape_task import ScrapeBBRefGamesForDate
from vigorish.scrape.brooks_games_for_date.scrape_task import ScrapeBrooksGamesForDate
from vigorish.scrape.brooks_pitch_logs.scrape_task import ScrapeBrooksPitchLogs
from vigorish.scrape.brooks_pitchfx.scrape_task import ScrapeBrooksPitchFx
from vigorish.status.report_status import report_date_range_status, report_season_status
from vigorish.util.dt_format_strings import DATE_ONLY_2
from vigorish.util.result import Result
from vigorish.util.sys_helpers import node_is_installed, node_modules_folder_exists

JOB_STATUS_TEXT_COLOR = {"scraped": "bright_green", "skipped": "blue"}
NODEJS_INSTALL_ERROR = (
    "Node.js installation cannot be located. Please install a recent, stable "
    "version and install dependencies from npm, see github repo for detailed "
    "intructions."
)
NPM_PACKAGES_INSTALL_ERROR = (
    "Nightmare is not installed, you must install it and other node dependencies in "
    "order to scrape any data."
)


class JobRunner:
    def __init__(self, db_job, db_session, config, scraped_data):
        self.db_job = db_job
        self.job_id = self.db_job.id
        self.job_name = self.db_job.name
        self.data_sets = self.db_job.data_sets
        self.start_date = self.db_job.start_date
        self.end_date = self.db_job.end_date
        self.season = self.db_job.season
        self.db_session = db_session
        self.config = config
        self.scraped_data = scraped_data
        self.spinners = defaultdict(lambda: Halo(spinner=FAKE_SPINNER))
        self.task_results = []
        self.status_report = self.config.get_current_setting("STATUS_REPORT", DataSet.ALL)

    def execute(self):
        result = self.initialize()
        if result.failure:
            return self.job_failed(result)
        for task_number, data_set in enumerate(self.data_sets, start=1):
            self.report_task_results()
            self.update_spinner(data_set, task_number)
            result = self.get_scrape_task_for_data_set(data_set).execute()
            if result.failure:
                if "skip" in result.error:
                    self.log_result_data_set_skipped(data_set, task_number)
                    self.spinners[data_set].stop()
                    continue
                return self.job_failed(result, data_set)
            self.log_result_data_set_scraped(data_set, task_number)
            self.spinners[data_set].stop()
        self.report_task_results()
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

    def update_spinner(self, data_set, task_number):
        self.spinners[data_set].text = self.scrape_task_started_text(data_set, task_number)
        self.spinners[data_set].color = JOB_SPINNER_COLORS[data_set]
        self.spinners[data_set].start()
        self.spinners[data_set].stop_and_persist(self.spinners[data_set].frame(), "")

    def log_result_data_set_scraped(self, data_set, task_number):
        self.task_results.append(self.scrape_task_complete_text(data_set, task_number))

    def log_result_data_set_skipped(self, data_set, task_number):
        self.task_results.append(self.scrape_task_skipped_text(data_set, task_number))

    def scrape_task_complete_text(self, data_set, task_number):
        scraped = f"Scraped data set: {data_set.name} (Task {task_number}/{len(self.data_sets)})"
        return (scraped, JOB_STATUS_TEXT_COLOR["scraped"])

    def scrape_task_skipped_text(self, data_set, task_number):
        skipped = f"Skipped data set: {data_set.name} (Task {task_number}/{len(self.data_sets)})"
        return (skipped, JOB_STATUS_TEXT_COLOR["skipped"])

    def scrape_task_started_text(self, data_set, task_number):
        return f"Scraping data set: {data_set.name} (Task {task_number}/{len(self.data_sets)})"

    def report_task_results(self):
        subprocess.run(["clear"])
        start_date_str = self.start_date.strftime(DATE_ONLY_2)
        end_date_str = self.end_date.strftime(DATE_ONLY_2)
        job_name_id = f"Job Name: {self.job_name} (ID: {self.job_id.upper()})"
        if self.job_name == self.job_id:
            job_name_id = f"Job ID: {self.job_id.upper()}"
        job_heading = f"{job_name_id} Date Range: {start_date_str}-{end_date_str}\n"
        print_message(job_heading, wrap=False, fg="bright_yellow", bold=True, underline=True)
        if not self.task_results:
            return
        for task_result in self.task_results:
            print_message(task_result[0], fg=task_result[1])

    def initialize(self):
        errors = []
        if not node_is_installed():
            errors.append(NODEJS_INSTALL_ERROR)
        elif not node_modules_folder_exists():
            errors.append(NPM_PACKAGES_INSTALL_ERROR)
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
            self.start_time = datetime.now()
            return Result.Ok()
        return Result.Fail("\n".join(errors))

    def show_status_report(self):
        if self.status_report == StatusReport.SEASON_SUMMARY:
            return report_season_status(
                db_session=self.db_session,
                year=self.season.year,
                report_type=self.status_report,
            )
        return report_date_range_status(
            db_session=self.db_session,
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
        shutil.rmtree(self.db_job.scraped_html_root_folder)
        return Result.Ok()

    def create_all_folderpaths(self):
        return self.scraped_data.create_all_folderpaths(self.season.year)

    def check_url_delay_settings(self):
        return self.config.check_url_delay_settings(self.data_sets)

    def s3_bucket_required(self):
        return self.config.s3_bucket_required(self.data_sets)

    def check_s3_bucket(self):
        return self.scraped_data.check_s3_bucket()
