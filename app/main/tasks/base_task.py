class BaseTask:
    key_name = ""
    display_name = ""


    def __init__(self, db, season):
        self.db = db
        self.season = season
        self.driver = None
