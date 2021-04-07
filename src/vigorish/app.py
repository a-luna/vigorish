import os
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import Optional

from sqlalchemy import create_engine, func
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.schema import Table
from vigorish.data.game_data import GameData

import vigorish.database as db
import vigorish.setup.populate_tables as setup_db
from vigorish.config.config_file import ConfigFile
from vigorish.config.config_setting import ConfigSettingValue, PathConfigSetting
from vigorish.config.dotenv_file import DotEnvFile
from vigorish.config.project_paths import CSV_FOLDER, JSON_FOLDER, SQLITE_DEV_URL, SQLITE_PROD_URL
from vigorish.data.scraped_data import ScrapedData
from vigorish.enums import DataSet
from vigorish.types import AuditReport
from vigorish.util.result import Result


class Vigorish:
    dotenv: DotEnvFile
    config: ConfigFile
    db_engine: Engine
    db_session: Session
    scraped_data: ScrapedData

    def __init__(
        self,
        dotenv_file: Optional[Path] = None,
        db_engine: Optional[Engine] = None,
        db_session: Optional[Session] = None,
    ) -> None:
        self.initialize_app(dotenv_file, db_engine, db_session)
        os.environ["INTERACTIVE_MODE"] = "NO" if "TEST" in os.environ.get("ENV", "DEV") else "YES"

    @property
    def dotenv_filepath(self) -> Path:
        return self.dotenv.dotenv_filepath

    @property
    def db_setup_complete(self) -> bool:
        tables_missing = (
            "player" not in self.db_engine.table_names()
            or "season" not in self.db_engine.table_names()
            or "team" not in self.db_engine.table_names()
        )
        if tables_missing:
            return False
        return (
            self.get_total_number_of_rows(db.Season) > 0
            and self.get_total_number_of_rows(db.Player) > 0
            and self.get_total_number_of_rows(db.Team) > 0
        )

    @cached_property
    def audit_report(self) -> AuditReport:
        return self.scraped_data.get_audit_report()

    def initialize_app(
        self,
        dotenv_file: Optional[Path] = None,
        db_engine: Optional[Engine] = None,
        db_session: Optional[Session] = None,
    ) -> None:
        self.dotenv = DotEnvFile(dotenv_filepath=dotenv_file)
        self.db_url = _get_db_url()
        self.config = ConfigFile()
        self.db_engine = db_engine or self._create_db_engine()
        self.db_session = db_session or self._create_db_session()
        self.scraped_data = ScrapedData(self.db_engine, self.db_session, self.config)

    def get_total_number_of_rows(self, db_table: Table) -> int:
        q = self.db_session.query(db_table)
        count_q = q.statement.with_only_columns([func.count()]).order_by(None)
        return q.session.execute(count_q).scalar()

    def initialize_database(self, csv_folder: Optional[Path] = None, json_folder: Optional[Path] = None) -> Result:
        if not csv_folder:
            csv_folder = CSV_FOLDER
        if not json_folder:
            json_folder = JSON_FOLDER
        self._create_db_schema()
        return setup_db.populate_tables(self, csv_folder, json_folder)

    def prepare_database_for_restore(
        self, csv_folder: Optional[Path] = None, json_folder: Optional[Path] = None
    ) -> Result:
        if not csv_folder:
            csv_folder = CSV_FOLDER
        if not json_folder:
            json_folder = JSON_FOLDER
        self._delete_db_file()
        self.reset_database_connection()
        self._create_db_schema()
        return setup_db.populate_tables_for_restore(self, csv_folder, json_folder)

    def reset_database_connection(self) -> None:
        self.db_session.close()
        self.db_session = None
        self.initialize_app(self.dotenv_filepath)

    def create_scrape_job(
        self, data_set: DataSet, start_date: datetime, end_date: datetime, job_name: Optional[str] = None
    ) -> Result[db.ScrapeJob]:
        result = db.Season.validate_date_range(self.db_session, start_date, end_date)
        if result.failure:
            return result
        season = result.value
        new_job = db.ScrapeJob.from_user_params(self.db_session, data_set, start_date, end_date, season, job_name)
        return Result.Ok(new_job)

    def get_current_setting(self, setting_name, data_set=DataSet.ALL, year=None) -> ConfigSettingValue:
        config_setting = self.config.all_settings.get(setting_name)
        if not config_setting:
            raise ValueError(f"{setting_name} is not a valid configuration setting")
        return (
            config_setting.current_setting(data_set).resolve(year)
            if isinstance(config_setting, PathConfigSetting)
            else config_setting.current_setting(data_set)
        )

    def get_scoreboard_data_for_date(self, game_date: datetime):
        game_ids = db.DateScrapeStatus.get_all_bbref_game_ids_for_date(self.db_session, game_date)
        return [GameData(self, game_id).get_game_data() for game_id in game_ids]

    def _create_db_engine(self) -> Engine:
        return create_engine(self.db_url, connect_args={"check_same_thread": False})

    def _create_db_session(self) -> Session:
        return sessionmaker(bind=self.db_engine)()

    def _create_db_schema(self) -> None:
        db.Base.metadata.drop_all(self.db_engine)
        db.Base.metadata.create_all(self.db_engine)

    def _delete_db_file(self) -> None:
        db_file = Path(self.db_url.replace("sqlite:///", ""))
        if db_file.exists():
            db_file.unlink()


def _get_db_url() -> str:
    db_url = os.getenv("DATABASE_URL", "")
    if db_url and db_url.startswith("/"):
        db_url = f"sqlite:///{db_url}"
    env = os.getenv("ENV", "prod")
    return db_url or (SQLITE_DEV_URL if env == "dev" else SQLITE_PROD_URL)
