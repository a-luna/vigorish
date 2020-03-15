from vigorish.scrape.scrape_task import ScrapeTaskABC


class ScrapeBrooksPitchLogsForDate(ScrapeTaskABC):
    def check_prerequisites(self):
        pass

    def check_current_status(self):
        pass

    def validate_local_folder_paths(self):
        pass

    def get_required_input_data(self):
        pass

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
