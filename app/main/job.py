import time
from datetime import datetime
from random import randint

from tqdm import tqdm
from selenium.common.exceptions import NoSuchWindowException

from app.main.constants import PBAR_LEN_DICT
from app.main.models.season import Season
from app.main.task_list import get_task_list
from app.main.util.datetime_util import get_date_range, format_timedelta
from app.main.util.decorators import RetryLimitExceededError
from app.main.util.dt_format_strings import MONTH_NAME_SHORT
from app.main.util.result import Result
from app.main.util.scrape_functions import get_chromedriver


class ScrapeJob:

    def __init__(self, db, data_set, start_date, end_date):
        self.db = db
        self.data_set = data_set
        self.start_date = start_date
        self.end_date = end_date
        self.status = "Not Started"
        self.errors = []


    @property
    def duration(self):
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None


    @property
    def status_report(self):
        if self.status == "Succeeded":
            start_str = self.start_date.strftime(MONTH_NAME_SHORT)
            end_str = self.end_date.strftime(MONTH_NAME_SHORT)
            report = (
                "Requested data was successfully scraped:\n"
                f"data set....: {self.data_set}\n"
                f"date range..: {start_str} - {end_str}\n"
                f"duration....: {format_timedelta(self.duration)}")
            if self.errors:
                errors = "\n".join(self.errors)
                report += f"\nerrors......: {errors}"
            return report
        elif self.status == "Failed":
            return str(self.result)
        else:
            return self.status


    def run(self):
        result = self._initialize()
        if result.failure:
            return self._job_failed(result)
        print() # place an empty line between the command and the progress bars
        self.status = "In Progress"
        self.start_time = datetime.now()
        with tqdm(total=len(self.date_range), unit="day", position=0, leave=False) as pbar_date:
            for scrape_date in self.date_range:
                with tqdm(total=len(self.task_list), unit="data-set", position=1, leave=False) as pbar_data_set:
                    for task in self.task_list:
                        scrape_task = task(self.db, self.season, self.driver)
                        pbar_date.set_description(self._get_pbar_date_description(scrape_date, scrape_task.key_name))
                        pbar_data_set.set_description(self._get_pbar_data_set_description(scrape_task.key_name))
                        result = scrape_task.execute(scrape_date)
                        if result.failure:
                            return self._job_failed(result)
                        self.db['session'].commit()
                        time.sleep(randint(250, 300) / 100.0)
                        pbar_data_set.update()
                pbar_date.update()
        return self._job_succeeded()


    def _initialize(self):
        try:
            self.driver = get_chromedriver()
        except RetryLimitExceededError as e:
            return Result.Fail(repr(e))
        except Exception as e:
            return Result.Fail(f"Error: {repr(e)}")
        result = Season.validate_date_range(self.db['session'], self.start_date, self.end_date)
        if result.failure:
            return result
        self.season = result.value
        result = get_task_list(self.data_set)
        if result.failure:
            return result
        self.task_list = result.value
        self.date_range = get_date_range(self.start_date, self.end_date)
        return Result.Ok()


    def _get_pbar_date_description(self, date, data_set):
        pre =f"Game Date | {date.strftime(MONTH_NAME_SHORT)}"
        pad_len = PBAR_LEN_DICT[data_set] - len(pre)
        return f"{pre}{'.'*pad_len}"


    def _get_pbar_data_set_description(self, data_set):
        pre = f"Data Set  | {data_set}"
        pad_len = PBAR_LEN_DICT[data_set] - len(pre)
        return f"{pre}{'.'*pad_len}"


    def _tear_down(self, status, result):
        self.end_time = datetime.now()
        self.status = status
        self.result = result
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
            except Exception as e:
                self.errors += f"Error occurred quitting chromedriver: {repr(e)}"
        return self.result


    def _job_failed(self, result):
        self.errors += result.error
        return self._tear_down("Failed", result)


    def _job_succeeded(self):
        return self._tear_down("Succeeded", Result.Ok())
