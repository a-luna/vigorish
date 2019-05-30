import time
from datetime import datetime
from random import randint

from tqdm import tqdm

from app.main.constants import PBAR_LEN_DICT
from app.main.models.season import Season
from app.main.task_list import get_task_list
from app.main.util.datetime_util import get_date_range
from app.main.util.dt_format_strings import MONTH_NAME_SHORT
from app.main.util.result import Result
from app.main.util.scrape_functions import get_chromedriver


class ScrapeJob:

    def __init__(self, db, data_set, start_date, end_date):
        self.db = db
        self.data_set = data_set
        self.start_date = start_date
        self.end_date = end_date


    def initialize(self):
        result = Season.validate_date_range(self.db['session'], self.start_date, self.end_date)
        if result.failure:
            return result
        self.season = result.value
        self.date_range = get_date_range(self.start_date, self.end_date)
        result = get_chromedriver()
        if result.failure:
            return result
        self.driver = result.value
        result = get_task_list(self.data_set)
        if result.failure:
            return result
        self.task_list = result.value
        return Result.Ok()

    def run(self):
        result = self.initialize()
        if result.failure:
            return result
        print() # place an empty line between the command and the progress bars
        start_time = datetime.now()
        with tqdm(total=len(self.date_range), unit="day", position=0, leave=False) as pbar_date:
            for scrape_date in self.date_range:
                with tqdm(total=len(self.task_list), unit="data-set", position=1, leave=False) as pbar_data_set:
                    for task in self.task_list:
                        scrape_task = task(self.db, self.season, self.driver)
                        pbar_date.set_description(self.get_pbar_date_description(scrape_date, scrape_task.key_name))
                        pbar_data_set.set_description(self.get_pbar_data_set_description(scrape_task.key_name))
                        result = scrape_task.execute(scrape_date)
                        if result.failure:
                            self.free_resources()
                            return result
                        time.sleep(randint(250, 300) / 100.0)
                        pbar_data_set.update()
                pbar_date.update()
        end_time = datetime.now()
        self.duration = end_time - start_time
        self.free_resources()
        return Result.Ok()


    def get_pbar_date_description(self, date, data_set):
        pre =f"Game Date | {date.strftime(MONTH_NAME_SHORT)}"
        pad_len = PBAR_LEN_DICT[data_set] - len(pre)
        return f"{pre}{'.'*pad_len}"


    def get_pbar_data_set_description(self, data_set):
        pre = f"Data Set  | {data_set}"
        pad_len = PBAR_LEN_DICT[data_set] - len(pre)
        return f"{pre}{'.'*pad_len}"


    def free_resources(self):
        self.driver.close()
        self.driver.quit()
        self.driver = None
