"""Total runs, hits and errors by a team and by the opponent team in a single game."""
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship

from vigorish.config.database import Base
from vigorish.util.list_helpers import display_dict


class GameTeamTotals(Base):
    """Total runs, hits and errors by a team and by the opponent team in a single game."""

    __tablename__ = "game_team_totals"
    id = Column(Integer, primary_key=True)
    team_id_br = Column(String)
    opponent_id_br = Column(String)
    total_wins_before_game = Column(Integer)
    total_losses_before_game = Column(Integer)
    total_runs_scored_by_team = Column(Integer)
    total_runs_scored_by_opponent = Column(Integer)
    total_hits_by_team = Column(Integer)
    total_hits_by_opponent = Column(Integer)
    total_errors_by_team = Column(Integer)
    total_errors_by_opponent = Column(Integer)
    bbref_game_id = Column(String)
    boxscore_id = Column(Integer, ForeignKey("boxscore.id"))
    away_team_id = Column(Integer, ForeignKey("team.id"))
    home_team_id = Column(Integer, ForeignKey("team.id"))

    boxscore = relationship("Boxscore")
    away_team = relationship("Team", foreign_keys=[away_team_id])
    home_team = relationship("Team", foreign_keys=[home_team_id])

    def __repr__(self):
        return f"<GameTeamTotals bbref_game_id={self.bbref_game_id}>"

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def display(self):
        team_totals_dict = self.as_dict()
        title = f"Game team totals for {self.team_id_br}, game: {self.bbref_game_id}"
        display_dict(team_totals_dict, title=title)
