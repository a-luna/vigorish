"""Values used to identify a single player in various websites and databases."""
from sqlalchemy import Column, Boolean, Integer, String, Date, ForeignKey
from sqlalchemy.orm import relationship

from app.main.models.base import Base


class PlayerId(Base):
    """Values used to identify a single player in various websites and databases."""

    __tablename__ = "player_id"
    mlb_id = Column(Integer, primary_key=True)
    mlb_name = Column(String)
    bp_id = Column(Integer)
    bbref_id = Column(String, index=True)
    bbref_name = Column(String)
    espn_id = Column(Integer)
    espn_name = Column(String)
    fg_id = Column(String)
    fg_name = Column(String)
    lahman_id = Column(String)
    nfbc_id = Column(Integer)
    retro_id = Column(String, index=True)
    yahoo_id = Column(Integer)
    yahoo_name = Column(String)
    ottoneu_id = Column(Integer)
    rotowire_id = Column(Integer)
    rotowire_name = Column(String)
    db_player_id = Column(Integer, ForeignKey("player.id"))

    def __repr__(self):
        return f"<PlayerId bbref_id={self.bbref_id}, mlb_id={self.mlb_id}>"

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    @classmethod
    def find_by_retro_id(cls, session, retro_id):
        return session.query(cls).filter_by(retro_id=retro_id).first()

    @classmethod
    def find_by_bbref_id(cls, session, bbref_id):
        return session.query(cls).filter_by(bbref_id=bbref_id).first()

    @classmethod
    def find_by_mlb_id(cls, session, mlb_id):
        return session.query(cls).filter_by(mlb_id=mlb_id).first()
