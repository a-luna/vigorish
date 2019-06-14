class BaseTask:
    key_name = ""
    display_name = ""


    def __init__(self, db, season, use_selenium=False):
        self.db = db
        self.season = season
        if not use_selenium:
            self.driver = None
        else:
            try:
                self.driver = get_chromedriver()
            except RetryLimitExceededError as e:
                self.driver = None
            except Exception as e:
                self.driver = None
