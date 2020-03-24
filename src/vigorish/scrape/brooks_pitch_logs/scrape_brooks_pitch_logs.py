from vigorish.enums import DataSet
from vigorish.scrape.scrape_task import ScrapeTaskABC


class ScrapeBrooksPitchLogs(ScrapeTaskABC):
    def __init__(self, db_job, db_session, config, scraped_data, driver, url_builder):
        self.data_set = DataSet.BROOKS_PITCH_LOGS
        super().__init__(db_job, db_session, config, scraped_data, driver, url_builder)
