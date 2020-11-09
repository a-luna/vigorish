"""Database file and connection details."""
# flake8: noqa
import os
from pathlib import Path

from sqlalchemy import create_engine, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

from vigorish.config.project_paths import CSV_FOLDER, VIG_FOLDER
from vigorish.models.pitchfx import PitchFx
from vigorish.models.player import Player
from vigorish.models.player_id import PlayerId
from vigorish.models.scrape_error import ScrapeError
from vigorish.models.scrape_job import ScrapeJob
from vigorish.models.season import Season
from vigorish.models.status_date import DateScrapeStatus
from vigorish.models.status_game import GameScrapeStatus
from vigorish.models.status_pitch_appearance import PitchAppScrapeStatus
from vigorish.models.team import Team
from vigorish.models.time_between_pitches import TimeBetweenPitches
from vigorish.models.views.date_pitch_app_view import Date_PitchApp_View
from vigorish.models.views.game_pitch_app_view import Game_PitchApp_View
from vigorish.models.views.pitch_app_pitchfx_view import PitchApp_PitchFx_View
from vigorish.models.views.player_pitchfx_view import (
    Pitch_Mix_All_View,
    Pitch_Mix_By_Year_View,
    Pitch_Mix_Left_View,
    Pitch_Mix_Right_View,
)
from vigorish.models.views.season_date_view import Season_Date_View
from vigorish.models.views.season_game_pitch_app_view import Season_Game_PitchApp_View
from vigorish.models.views.season_game_view import Season_Game_View
from vigorish.models.views.season_pitch_app_view import Season_PitchApp_View
from vigorish.setup.populate_tables import populate_tables, populate_tables_for_restore
from vigorish.util.result import Result

SQLITE_DEV_URL = f"sqlite:///{VIG_FOLDER.joinpath('vig_dev.db')}"
SQLITE_PROD_URL = f"sqlite:///{VIG_FOLDER.joinpath('vig.db')}"


def get_db_url():
    if os.environ.get("ENV") == "TEST":
        return os.environ.get("DATABASE_URL")
    db_url = os.getenv("DATABASE_URL", "")
    if db_url and db_url.startswith("/"):
        db_url = f"sqlite:///{db_url}"
    env = os.getenv("ENV", "prod")
    return db_url if db_url else SQLITE_DEV_URL if env == "dev" else SQLITE_PROD_URL


def initialize_database(app, csv_folder=None):
    if not csv_folder:
        csv_folder = CSV_FOLDER
    Base.metadata.drop_all(app["db_engine"])
    Base.metadata.create_all(app["db_engine"])
    return populate_tables(app, csv_folder)


def prepare_database_for_restore(app, csv_folder=None):
    if not csv_folder:
        csv_folder = CSV_FOLDER
    Base.metadata.drop_all(app["db_engine"])
    Base.metadata.create_all(app["db_engine"])
    return populate_tables_for_restore(app, csv_folder)


def reset_database_connection(app, db_url=None):
    if not db_url:
        db_url = get_db_url()
    result = delete_sqlite_database(db_url)
    if result.failure:
        return result
    app = update_app_instance(app, db_url)
    return Result.Ok(app)


def delete_sqlite_database(db_url):
    db_file = Path(db_url.replace("sqlite:///", ""))
    if not db_file.exists():
        return Result.Fail("Error occurred attempting to delete existing database file!")
    db_file.unlink()
    return Result.Ok()


def update_app_instance(app, db_url):
    app["db_session"].close()
    app["db_session"] = None
    db_engine = create_engine(db_url)
    session_maker = sessionmaker(bind=db_engine)
    db_session = session_maker()
    dotenv = app["dotenv"]
    config = app["config"]
    scraped_data = app["scraped_data"]
    scraped_data.db_engine = db_engine
    scraped_data.db_session = db_session
    return {
        "dotenv": dotenv,
        "config": config,
        "db_engine": db_engine,
        "db_session": db_session,
        "scraped_data": scraped_data,
    }


def db_setup_complete(db_engine, db_session):
    tables_missing = (
        "player" not in db_engine.table_names()
        or "season" not in db_engine.table_names()
        or "team" not in db_engine.table_names()
    )
    if tables_missing:
        return False
    return (
        len(db_session.query(Season).all()) > 0
        and len(db_session.query(Player).all()) > 0
        and len(db_session.query(Team).all()) > 0
    )


def get_total_number_of_rows(db_session, db_table):
    q = db_session.query(db_table)
    count_q = q.statement.with_only_columns([func.count()]).order_by(None)
    count = q.session.execute(count_q).scalar()
    return count
