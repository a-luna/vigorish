"""Database file and connection details."""
# flake8: noqa
import os
from pathlib import Path

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

from vigorish.models.boxscore import Boxscore
from vigorish.models.game_bat_stats import GameBatStats
from vigorish.models.game_event import GameEvent
from vigorish.models.game_inning import GameHalfInning
from vigorish.models.game_meta import GameMetaInformation
from vigorish.models.game_pitch_stats import GamePitchStats
from vigorish.models.game_starting_lineup import GameStartingLineupSlot
from vigorish.models.game_substitution import GameSubstitution
from vigorish.models.game_team_totals import GameTeamTotals
from vigorish.models.player import Player
from vigorish.models.player_id import PlayerId
from vigorish.models.runners_on_base import RunnersOnBase
from vigorish.models.scrape_job import ScrapeJob
from vigorish.models.scrape_error import ScrapeError
from vigorish.models.season import Season
from vigorish.models.status_date import DateScrapeStatus
from vigorish.models.status_game import GameScrapeStatus
from vigorish.models.status_pitch_appearance import PitchAppScrapeStatus
from vigorish.models.team import Team
from vigorish.setup.populate_tables import populate_tables


VIG_FOLDER = Path.home() / ".vig"
SQLITE_DEV_URL = f"sqlite:///{VIG_FOLDER / 'vig_dev.db'}"
SQLITE_PROD_URL = f"sqlite:///{VIG_FOLDER / 'vig.db'}"


def get_db_url():
    db_url = os.getenv("DATABASE_URL", "")
    if db_url and db_url.startswith("/"):
        db_url = f"sqlite:///{db_url}"
    env = os.getenv("ENV", "prod")
    return db_url if db_url else SQLITE_DEV_URL if env == "dev" else SQLITE_PROD_URL


def initialize_database(db_engine, db_session):
    Base.metadata.drop_all(db_engine)
    Base.metadata.create_all(db_engine)
    return populate_tables(db_session)


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
