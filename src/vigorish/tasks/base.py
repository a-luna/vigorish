from abc import ABC, abstractmethod


class Task(ABC):
    """Base class for a task that requires access to the db and application state."""

    def __init__(self, app):
        self.app = app
        self.dotenv = app["dotenv"]
        self.config = app["config"]
        self.db_engine = app["db_engine"]
        self.db_session = app["db_session"]
        self.scraped_data = app["scraped_data"]

    @abstractmethod
    def execute(self, *args, **kwargs):
        pass
