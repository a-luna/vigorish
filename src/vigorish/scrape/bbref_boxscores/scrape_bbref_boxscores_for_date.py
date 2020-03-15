from vigorish.config.database import DateScrapeStatus
from vigorish.enums import DataSet, HtmlStorageOption, JsonStorage, ScrapeCondition
from vigorish.scrape.scrape_task import ScrapeTaskABC
from vigorish.util.result import Result


class ScrapeBBRefBoxscoresForDate(ScrapeTaskABC):
    def __init__(self, db, season, config, s3, file_helper, driver):
        self.data_set = DataSet.BBREF_BOXSCORES
        super().__init__(db, season, config, s3, file_helper, driver)

    def check_prerequisites(self):
        scraped_bbref_games_for_date = DateScrapeStatus.verify_bbref_daily_dashboard_scraped_for_date(
            self.db, scrape_date
        )
        if scraped_bbref_games_for_date:
            return Result.Ok()
        date_str = scrape_date.strftime(DATE_ONLY)
        error = (
            f"BBref games for date {date_str} have not been scraped, unable to scrape BBref "
            "boxscores until this has been done."
        )
        return Result.Fail(error)

    def check_current_status(self):
        if self.scrape_condition == ScrapeCondition.NEVER:
            return Result.Fail("skip")
        scraped_bbref_boxscores = DateScrapeStatus.verify_all_bbref_boxscores_scraped_for_date(
            self.db, scrape_date
        )
        if scraped_bbref_boxscores and self.scrape_condition == ScrapeCondition.ONLY_MISSING_DATA:
            return Result.Fail("skip")
        return Result.Ok()

    def validate_local_folder_paths(self):
        html_folder_is_valid = True
        json_folder_is_valid = True
        if (
            self.html_storage == HtmlStorageOption.LOCAL_FOLDER
            or self.html_storage == HtmlStorageOption.BOTH
        ):
            html_folder_is_valid = self.html_folder.is_valid(year=self.season.year)
        if self.json_storage == JsonStorage.LOCAL_FOLDER or self.json_storage == JsonStorage.BOTH:
            json_folder_is_valid = self.json_folder.is_valid(year=self.season.year)
        if html_folder_is_valid and json_folder_is_valid:
            return Result.Ok()

        errors = []
        if not html_folder_is_valid:
            folder_path = self.html_folder.resolve(year=self.season.year)
            errors.append(f"Error occurred validating html folder path: {folder_path}")
        if not json_folder_is_valid:
            folder_path = self.html_folder.resolve(year=self.season.year)
            errors.append(f"Error occurred validating json folder path: {folder_path}")
        return Result.Fail("\n".join(errors))

    def get_required_input_data(self):
        result = self.file_helper.get_bbref_games_for_date(scrape_date)
        if result.failure:
            result = self.s3.get_bbref_games_for_date(scrape_date)
            if result.failure:
                return result
        self.bbref_games_for_date = result.value
        return Result.Ok()

    def construct_url_set(self):
        pass

    def get_html_from_local_foldera(self):
        pass

    def get_html_from_s3(self):
        pass

    def scrape_html_with_requests_selenium(self):
        pass

    def scrape_html_with_nightmarejs(self):
        pass

    def save_html_to_local_folder(self):
        pass

    def save_html_to_s3(self):
        pass

    def parse_json_data_from_html(self):
        pass

    def save_json_to_local_folder(self):
        pass

    def save_json_to_s3(self):
        pass

    def update_status(self):
        pass
