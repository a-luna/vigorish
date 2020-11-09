"""Biographical information for a single player."""
from sqlalchemy import Boolean, Column, DateTime, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import backref, relationship

from vigorish.config.database import Base
from vigorish.util.list_helpers import display_dict
from vigorish.util.pitch_calcs import calc_pitch_mix


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
    retro_id = Column(String, default="")
    mlb_id = Column(Integer, index=True)
    scraped_transactions = Column(Boolean, default=False)
    minor_league_player = Column(Boolean, default=False)
    missing_mlb_id = Column(Boolean, default=True)

    id_map = relationship("PlayerId", backref=backref("player", uselist=False))
    pitch_apps = relationship("PitchAppScrapeStatus", backref="player")
    pmix_all_view = relationship(
        "Pitch_Mix_All_View",
        uselist=False,
        primaryjoin="Player.id==Pitch_Mix_All_View.id",
        foreign_keys="Pitch_Mix_All_View.id",
    )
    pmix_right_view = relationship(
        "Pitch_Mix_Right_View",
        uselist=False,
        primaryjoin="Player.id==Pitch_Mix_Right_View.id",
        foreign_keys="Pitch_Mix_Right_View.id",
    )
    pmix_left_view = relationship(
        "Pitch_Mix_Left_View",
        uselist=False,
        primaryjoin="Player.id==Pitch_Mix_Left_View.id",
        foreign_keys="Pitch_Mix_Left_View.id",
    )

    def __repr__(self):
        return f"<Player name={self.name_first} {self.name_last}, bbref_id={self.bbref_id}>"

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def display(self):
        display_dict(self.as_dict())

    @hybrid_property
    def pitch_mix(self):
        return calc_pitch_mix(self.pmix_all_view.__dict__, self.pmix_all_view.total_pitches)

    @hybrid_property
    def pitch_mix_right(self):
        return calc_pitch_mix(self.pmix_right_view.__dict__, self.pmix_right_view.total_pitches)

    @hybrid_property
    def pitch_mix_left(self):
        return calc_pitch_mix(self.pmix_left_view.__dict__, self.pmix_left_view.total_pitches)

    @classmethod
    def find_by_bbref_id(cls, db_session, bbref_id):
        return db_session.query(cls).filter_by(bbref_id=bbref_id).first()

    @classmethod
    def find_by_mlb_id(cls, db_session, mlb_id):
        return db_session.query(cls).filter_by(mlb_id=mlb_id).first()
