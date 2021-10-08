"""Biographical information for a single player."""
from dataclasses import dataclass
from datetime import datetime

from dataclass_csv import accept_whitespaces
from dataclass_csv.decorators import dateformat
from sqlalchemy import Boolean, Column, DateTime, Integer, String
from sqlalchemy.orm import backref, relationship

import vigorish.database as db
from vigorish.util.dt_format_strings import DATE_ONLY


class Player(db.Base):
    """Biographical information for a single player."""

    __tablename__ = "player"
    id = Column(Integer, primary_key=True)
    name_first = Column(String)
    name_last = Column(String)
    name_given = Column(String)
    bats = Column(String)
    throws = Column(String)
    weight = Column(Integer)
    height = Column(Integer)
    debut = Column(DateTime)
    birth_year = Column(Integer)
    birth_month = Column(Integer)
    birth_day = Column(Integer)
    birth_country = Column(String)
    birth_state = Column(String)
    birth_city = Column(String)
    bbref_id = Column(String, index=True)
    retro_id = Column(String, default="")
    mlb_id = Column(Integer, index=True)
    scraped_transactions = Column(Boolean, default=False)
    minor_league_player = Column(Boolean, default=False)
    missing_mlb_id = Column(Boolean, default=False)
    add_to_db_backup = Column(Boolean, default=False)

    id_map = relationship("PlayerId", backref=backref("player", uselist=False))
    pitch_apps = relationship("PitchAppScrapeStatus", backref="player")
    teams = relationship("Team", secondary="player_team", back_populates="players")

    def __repr__(self):
        return f"<Player name={self.name_first} {self.name_last}, bbref_id={self.bbref_id}>"

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    @property
    def name(self):
        return f"{self.name_first} {self.name_last}"

    @classmethod
    def find_by_bbref_id(cls, db_session, bbref_id):
        return db_session.query(cls).filter_by(bbref_id=bbref_id).first()

    @classmethod
    def find_by_mlb_id(cls, db_session, mlb_id) -> "Player":
        return db_session.query(cls).filter_by(mlb_id=mlb_id).first()


@accept_whitespaces
@dateformat(DATE_ONLY)
@dataclass
class PlayerCsvRow:
    id: int = None
    name_first: str = None
    name_last: str = None
    name_given: str = None
    bats: str = None
    throws: str = None
    weight: int = None
    height: int = None
    debut: datetime = None
    birth_year: int = None
    birth_month: int = None
    birth_day: int = None
    birth_country: str = None
    birth_state: str = None
    birth_city: str = None
    bbref_id: str = None
    retro_id: str = None
    mlb_id: int = None
    scraped_transactions: bool = False
    minor_league_player: bool = False
    missing_mlb_id: bool = False
    add_to_db_backup: bool = False
