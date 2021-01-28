import os
from pathlib import Path

from sqlalchemy import create_engine, func
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from vigorish.config.config_file import ConfigFile
from vigorish.config.dotenv_file import DotEnvFile
from vigorish.config.project_paths import CSV_FOLDER, SQLITE_DEV_URL, SQLITE_PROD_URL
from vigorish.data.scraped_data import ScrapedData
from vigorish.database import Base, Player, Season, Team
from vigorish.setup.populate_tables import populate_tables, populate_tables_for_restore
from vigorish.util.result import Result


class Vigorish:
    dotenv: DotEnvFile
    config: ConfigFile
    db_engine: Engine
    db_session: Session
    scraped_data: ScrapedData

    def __init__(self, dotenv_file: str = None) -> None:
        self.initialize_app(dotenv_file)
        os.environ["INTERACTIVE_MODE"] = "YES" if os.environ.get("ENV") != "TEST" else "NO"

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
            self.get_total_number_of_rows(Season) > 0
            and self.get_total_number_of_rows(Player) > 0
            and self.get_total_number_of_rows(Team) > 0
        )

    def initialize_app(self, dotenv_file: str):
        self.dotenv = DotEnvFile(dotenv_filepath=dotenv_file)
        self.db_url = get_db_url()
        self.config = ConfigFile()
        self.db_engine = create_engine(self.db_url)
        session_maker = sessionmaker(bind=self.db_engine)
        self.db_session = session_maker()
        self.scraped_data = ScrapedData(self.db_engine, self.db_session, self.config)

    def get_total_number_of_rows(self, db_table):
        q = self.db_session.query(db_table)
        count_q = q.statement.with_only_columns([func.count()]).order_by(None)
        return q.session.execute(count_q).scalar()

    def initialize_database(self, csv_folder=None):
        if not csv_folder:
            csv_folder = CSV_FOLDER
        Base.metadata.drop_all(self.db_engine)
        Base.metadata.create_all(self.db_engine)
        return populate_tables(self, csv_folder)

    def prepare_database_for_restore(self, csv_folder=None):
        if not csv_folder:
            csv_folder = CSV_FOLDER
        result = delete_sqlite_database(self.db_url)
        if result.failure:
            return result
        self.reset_database_connection()
        Base.metadata.drop_all(self.db_engine)
        Base.metadata.create_all(self.db_engine)
        return populate_tables_for_restore(self, csv_folder)

    def reset_database_connection(self):
        self.db_session.close()
        self.db_session = None
        self.initialize_app(self.dotenv_filepath)


def get_db_url():
    db_url = os.getenv("DATABASE_URL", "")
    if db_url and db_url.startswith("/"):
        db_url = f"sqlite:///{db_url}"
    env = os.getenv("ENV", "prod")
    return db_url if db_url else SQLITE_DEV_URL if env == "dev" else SQLITE_PROD_URL


def delete_sqlite_database(db_url):
    db_file = Path(db_url.replace("sqlite:///", ""))
    if db_file.exists():
        db_file.unlink()
    return Result.Ok()
