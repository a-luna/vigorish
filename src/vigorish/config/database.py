"""Database file and connection details."""
# flake8: noqa
import os
from pathlib import Path

from sqlalchemy import func, create_engine
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
    dotenv = app["dotenv"]
    config = app["config"]
    app["db_session"].close()
    app["db_session"] = None
    db_url = get_db_url()
    db_file = db_url.replace("sqlite:///", "")
    Path(db_file).unlink()
    app["db_engine"] = create_engine(db_url)
    session_maker = sessionmaker(bind=app["db_engine"])
    app["db_session"] = session_maker()
    app["dotenv"] = dotenv
    app["config"] = config
    app["scraped_data"].db_engine = app["db_engine"]
    app["scraped_data"].db_session = app["db_session"]
    Base.metadata.drop_all(app["db_engine"])
    Base.metadata.create_all(app["db_engine"])
    result = populate_tables_for_restore(app, csv_folder)
    if result.failure:
        return result
    return Result.Ok(app)


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
