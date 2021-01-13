"""Database file and connection details."""
# flake8: noqa
import os
from pathlib import Path

from sqlalchemy import func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

from vigorish.config.project_paths import CSV_FOLDER, VIG_FOLDER
from vigorish.models import (
    Assoc_Player_Team,
    BatStats,
    BatStatsCsvRow,
    PitchStats,
    PitchStatsCsvRow,
    PitchFx,
    PitchFxCsvRow,
    Player,
    PlayerId,
    ScrapeError,
    ScrapeJob,
    Season,
    DateScrapeStatus,
    DateScrapeStatusCsvRow,
    GameScrapeStatus,
    GameScrapeStatusCsvRow,
    PitchAppScrapeStatus,
    PitchAppScrapeStatusCsvRow,
    Team,
    TimeBetweenPitches,
)
from vigorish.models.views import (
    Date_PitchApp_View,
    Game_PitchApp_View,
    PitchApp_PitchType_All_View,
    PitchApp_PitchType_Left_View,
    PitchApp_PitchType_Right_View,
    Pitch_Type_All_View,
    Pitch_Type_By_Year_View,
    Pitch_Type_Left_View,
    Pitch_Type_Right_View,
    Season_Date_View,
    Season_Game_PitchApp_View,
    Season_Game_View,
    Season_PitchApp_View,
)
from vigorish.setup.populate_tables import populate_tables, populate_tables_for_restore
from vigorish.util.result import Result

SQLITE_DEV_URL = f"sqlite:///{VIG_FOLDER.joinpath('vig_dev.db')}"
SQLITE_PROD_URL = f"sqlite:///{VIG_FOLDER.joinpath('vig.db')}"


def get_db_url():
    db_url = os.getenv("DATABASE_URL", "")
    if db_url and db_url.startswith("/"):
        db_url = f"sqlite:///{db_url}"
    env = os.getenv("ENV", "prod")
    return db_url if db_url else SQLITE_DEV_URL if env == "dev" else SQLITE_PROD_URL


def initialize_database(app, csv_folder=None):
    if not csv_folder:
        csv_folder = CSV_FOLDER
    Base.metadata.drop_all(app.db_engine)
    Base.metadata.create_all(app.db_engine)
    return populate_tables(app, csv_folder)


def prepare_database_for_restore(app, csv_folder=None):
    if not csv_folder:
        csv_folder = CSV_FOLDER
    result = delete_sqlite_database(app.db_url)
    if result.failure:
        return result
    app.reset_database_connection()
    Base.metadata.drop_all(app.db_engine)
    Base.metadata.create_all(app.db_engine)
    return populate_tables_for_restore(app, csv_folder)


def delete_sqlite_database(db_url):
    db_file = Path(db_url.replace("sqlite:///", ""))
    if db_file.exists():
        db_file.unlink()
    return Result.Ok()


def get_total_number_of_rows(db_session, db_table):
    q = db_session.query(db_table)
    count_q = q.statement.with_only_columns([func.count()]).order_by(None)
    count = q.session.execute(count_q).scalar()
    return count
