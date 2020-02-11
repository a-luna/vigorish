"""Biographical information for a single player."""
from sqlalchemy import Column, Boolean, Integer, String, DateTime
from sqlalchemy.orm import relationship, backref

from app.main.models.base import Base
from app.main.models.game_event import GameEvent
from app.main.models.player_id import PlayerId
from app.main.util.list_functions import display_dict


class Player(Base):
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
    retro_id = Column(String, index=True)
    mlb_id = Column(Integer, index=True)
    scraped_transactions = Column(Boolean, default=False)
    minor_league_player = Column(Boolean, default=False)
    missing_mlb_id = Column(Boolean, default=True)

    id_map = relationship("PlayerId", backref=backref("player", uselist=False))

    game_events_as_pitcher = relationship(
        "GameEvent",
        primaryjoin="Player.id==GameEvent.pitcher_id",
        back_populates="pitcher",
    )
    game_events_as_batter = relationship(
        "GameEvent",
        primaryjoin="Player.id==GameEvent.batter_id",
        back_populates="batter",
    )
    pitching_stats = relationship("GamePitchStats", backref="player")
    batting_stats = relationship("GameBatStats", backref="player")
    lineup_appearances = relationship("GameStartingLineupSlot", backref="player")
    # transactions = relationship(
    #    'PlayerTransaction',
    #    secondary='player_transaction_link',
    #    back_populates='players'
    # )

    def __repr__(self):
        return (
            f"<Player name={self.name_first} {self.name_last}, bbref_id={self.bbref_id}>"
        )

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def display(self):
        display_dict(self.as_dict())

    @classmethod
    def find_by_bbref_id(cls, session, bbref_id):
        return session.query(cls).filter_by(bbref_id=bbref_id).first()

    @classmethod
    def find_by_mlb_id(cls, session, mlb_id):
        return session.query(cls).filter_by(mlb_id=mlb_id).first()