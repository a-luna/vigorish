class BaseTask:
    key_name = ""
    display_name = ""


    def __init__(self, db, driver):
        self.db = db
        self.driver = driver
