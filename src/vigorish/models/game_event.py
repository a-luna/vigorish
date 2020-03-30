"""Db model that describes a single event, most likely an at bat."""
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship

from vigorish.config.database import Base
from vigorish.util.list_helpers import display_dict


class GameEvent(Base):
    """Db model that describes a single event, most likely an at bat."""

    __tablename__ = "game_event"
    id = Column(Integer, primary_key=True)
    play_description = Column(String)
    pbp_table_row_number = Column(Integer)
    score = Column(String)
    outs_before_play = Column(Integer)
    pitch_sequence = Column(String)
    runs_outs_result = Column(String)
    pitcher_id_br = Column(String)
    batter_id_br = Column(String)
    team_batting_id_br = Column(String)
    team_pitching_id_br = Column(String)
    bbref_game_id = Column(String)
    bb_game_id = Column(String)

    runners_on_base_id = Column(Integer, ForeignKey("rob.id"))
    db_inning_id = Column(Integer, ForeignKey("game_inning.id"))
    pitcher_id = Column(Integer, ForeignKey("player.id"))
    batter_id = Column(Integer, ForeignKey("player.id"))
    team_batting_id = Column(Integer, ForeignKey("team.id"))
    team_pitching_id = Column(Integer, ForeignKey("team.id"))
    boxscore_id = Column(Integer, ForeignKey("boxscore.id"))
    season_id = Column(Integer, ForeignKey("season.id"))

    runners_on_base = relationship("RunnersOnBase", foreign_keys=[runners_on_base_id])
    pitcher = relationship("Player", foreign_keys=[pitcher_id])
    batter = relationship("Player", foreign_keys=[batter_id])
    team_pitching = relationship("Team", foreign_keys=[team_pitching_id])
    team_batting = relationship("Team", foreign_keys=[team_batting_id])
    season = relationship("Season", foreign_keys=[season_id])

    def __repr__(self):
        return f"<GameEvent inning_id={self.db_inning_id}, pbp_number={self.pbp_table_row_number}>"

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def display(self):
        event_dict = self.as_dict()
        title = (
            f"Details for game event: {self.inning.inning_id} Row #: {self.pbp_table_row_number}"
        )
        display_dict(event_dict, title=title)
