from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from vigorish.config.config_file import ConfigFile
from vigorish.config.dotenv_file import DotEnvFile
from vigorish.data.scraped_data import ScrapedData
from vigorish.database import get_db_url, get_total_number_of_rows, Player, Season, Team


class Vigorish:
    dotenv: DotEnvFile
    config: ConfigFile
    db_engine: Engine
    db_session: Session
    scraped_data: ScrapedData

    def __init__(self, dotenv_file: str = None) -> None:
        self.initialize_app(dotenv_file)

    @property
    def dotenv_filepath(self):
        return self.dotenv.dotenv_filepath if self.dotenv else None

    @property
    def db_setup_complete(self):
        tables_missing = (
            "player" not in self.db_engine.table_names()
            or "season" not in self.db_engine.table_names()
            or "team" not in self.db_engine.table_names()
        )
        if tables_missing:
            return False
        return (
            get_total_number_of_rows(self.db_session, Season) > 0
            and get_total_number_of_rows(self.db_session, Player) > 0
            and get_total_number_of_rows(self.db_session, Team) > 0
        )

    def initialize_app(self, dotenv_file: str):
        self.dotenv = DotEnvFile(dotenv_filepath=dotenv_file)
        self.db_url = get_db_url()
        self.config = ConfigFile()
        self.db_engine = create_engine(self.db_url)
        session_maker = sessionmaker(bind=self.db_engine)
        self.db_session = session_maker()
        self.scraped_data = ScrapedData(self.db_engine, self.db_session, self.config)

    def reset_database_connection(self):
        self.db_session.close()
        self.db_session = None
        self.initialize_app(self.dotenv_filepath)
