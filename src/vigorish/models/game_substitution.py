"""Db model that describes an in game substitution."""
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.types import Enum

from vigorish.enums import DefensePosition
from vigorish.config.database import Base
from vigorish.util.list_helpers import display_dict


class GameSubstitution(Base):
    """Db model that describes an in game substitution."""

    __tablename__ = "game_sub"
    id = Column(Integer, primary_key=True)
    sub_description = Column(String)
    pbp_table_row_number = Column(Integer)
    lineup_slot = Column(Integer)
    incoming_player_id_br = Column(String)
    outgoing_player_id_br = Column(String)
    incoming_player_pos = Column(Enum(DefensePosition), default=DefensePosition.NONE)
    outgoing_player_pos = Column(Enum(DefensePosition), default=DefensePosition.NONE)
    bbref_game_id = Column(String)
    bb_game_id = Column(String)

    db_inning_id = Column(Integer, ForeignKey("game_inning.id"))
    incoming_player_id = Column(Integer, ForeignKey("player.id"))
    outgoing_player_id = Column(Integer, ForeignKey("player.id"))
    team_id = Column(Integer, ForeignKey("team.id"))
    lineup_id = Column(Integer, ForeignKey("game_starting_lineup.id"))
    boxscore_id = Column(Integer, ForeignKey("boxscore.id"))
    season_id = Column(Integer, ForeignKey("season.id"))

    incoming_player = relationship("Player", foreign_keys=[incoming_player_id])
    outgoing_player = relationship("Player", foreign_keys=[outgoing_player_id])
    team = relationship("Team")

    def __repr__(self):
        return (
            "<GameSubstitution "
            f"in_player_id={self.incoming_player_id_br}, "
            f"out_player_id={self.outgoing_player_id_br}, "
            f"inning_id={self.db_inning_id}, "
            f"pbp_number={self.pbp_table_row_number}>"
        )

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def display(self):
        event_dict = self.as_dict()
        title = (
            f"Details for game event: {self.inning.inning_id} Row #: {self.pbp_table_row_number}"
        )
        display_dict(event_dict, title=title)
