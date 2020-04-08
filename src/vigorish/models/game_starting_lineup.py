"""The starting lineup slot for a player in a single game."""
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.types import Enum

from vigorish.enums import DefensePosition
from vigorish.config.database import Base
from vigorish.util.list_helpers import display_dict


class GameStartingLineupSlot(Base):
    """The starting lineup slot for a player in a single game."""

    __tablename__ = "game_starting_lineup"
    id = Column(Integer, primary_key=True)
    player_id_br = Column(String)
    bat_order = Column(Integer)
    def_position = Column(Enum(DefensePosition), default=DefensePosition.NONE)
    player_team_id_br = Column(String)
    opponent_team_id_br = Column(String)
    bbref_game_id = Column(String)
    player_id = Column(Integer, ForeignKey("player.id"))
    player_team_id = Column(Integer, ForeignKey("team.id"))
    opponent_team_id = Column(Integer, ForeignKey("team.id"))
    boxscore_id = Column(Integer, ForeignKey("boxscore.id"))

    team = relationship("Team", foreign_keys=[player_team_id])
    opponent_team = relationship("Team", foreign_keys=[opponent_team_id])
    substitutions = relationship("GameSubstitution", backref="starting_lineup")

    def __repr__(self):
        return (
            "<GameStartingLineupSlot "
            f"game_id={self.bbref_game_id}, "
            f"player_id={self.player_id_br}, "
            f"bat_order={self.bat_order}, "
            f"def_position={self.def_position}>"
        )

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def display(self):
        lineup_slot_dict = self.as_dict()
        title = f"Starting lineup slot for player {self.player_id_br}, game: {self.bbref_game_id}"
        display_dict(lineup_slot_dict, title=title)
