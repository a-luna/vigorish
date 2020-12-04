from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from vigorish.config.config_file import ConfigFile
from vigorish.config.dotenv_file import DotEnvFile
from vigorish.data.scraped_data import ScrapedData
from vigorish.database import delete_sqlite_database, get_db_url
from vigorish.util.result import Result


class Vigorish:
    dotenv: DotEnvFile
    config: ConfigFile
    db_engine: Engine
    db_session: Session
    scraped_data: ScrapedData

    def __init__(self, db_url=None) -> None:
        self.initialize_app(db_url)

    def initialize_app(self, db_url):
        self.dotenv = DotEnvFile()
        self.config = ConfigFile()
        if not db_url:
            db_url = get_db_url()
        self.db_engine = create_engine(db_url)
        session_maker = sessionmaker(bind=self.db_engine)
        self.db_session = session_maker()
        self.scraped_data = ScrapedData(self.db_engine, self.db_session, self.config)

    def reset_database_connection(self, db_url=None):
        if not db_url:
            db_url = get_db_url()
        result = delete_sqlite_database(db_url)
        if result.failure:
            return result
        self.db_session.close()
        self.db_session = None
        self.initialize_app(db_url)
        return Result.Ok()
