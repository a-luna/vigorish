"""Db model that describes a half-inning of a MLB game."""
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship

from vigorish.config.database import Base
from vigorish.util.list_helpers import display_dict


class GameHalfInning(Base):
    """Db model that describes a half-inning of a MLB game."""

    __tablename__ = "game_inning"
    id = Column(Integer, primary_key=True)
    inning_id = Column(String)
    inning_label = Column(String)
    begin_inning_summary = Column(String)
    end_inning_summary = Column(String)
    total_hits = Column(Integer)
    total_runs = Column(Integer)
    total_errors = Column(Integer)
    total_left_on_base = Column(Integer)
    away_team_runs_after_inning = Column(Integer)
    home_team_runs_after_inning = Column(Integer)
    team_batting_id_br = Column(String)
    team_pitching_id_br = Column(String)
    bbref_game_id = Column(String)
    bb_game_id = Column(String)
    boxscore_id = Column(Integer, ForeignKey("boxscore.id"))

    game_events = relationship("GameEvent", backref="inning")
    substitutions = relationship("GameSubstitution", backref="inning")

    def __repr__(self):
        return f"<GameHalfInning inning_id={self.inning_id}>"

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def display(self):
        inning_dict = self.as_dict()
        title = f"Details for inning: {self.inning_id}"
        display_dict(inning_dict, title=title)
