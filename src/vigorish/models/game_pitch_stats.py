"""Total pitching statistics for a single player in a single game."""
from sqlalchemy import Column, Integer, Float, String, ForeignKey
from sqlalchemy.orm import relationship

from vigorish.config.database import Base
from vigorish.util.list_helpers import display_dict


class GamePitchStats(Base):
    """Total pitching statistics for a single player in a single game."""

    __tablename__ = "game_pitch_stats"
    id = Column(Integer, primary_key=True)
    innings_pitched = Column(Float)
    hits = Column(Integer)
    runs = Column(Integer)
    earned_runs = Column(Integer)
    bases_on_balls = Column(Integer)
    strikeouts = Column(Integer)
    homeruns = Column(Integer)
    batters_faced = Column(Integer)
    pitch_count = Column(Integer)
    strikes = Column(Integer)
    strikes_contact = Column(Integer)
    strikes_swinging = Column(Integer)
    strikes_looking = Column(Integer)
    ground_balls = Column(Integer)
    fly_balls = Column(Integer)
    line_drives = Column(Integer)
    unknown_type = Column(Integer)
    game_score = Column(Integer)
    inherited_runners = Column(Integer)
    inherited_scored = Column(Integer)
    wpa_pitch = Column(Float)
    avg_lvg_index = Column(Float)
    re24_pitch = Column(Float)
    player_id_br = Column(String)
    player_team_id_br = Column(String)
    opponent_team_id_br = Column(String)
    bbref_game_id = Column(String)
    bb_game_id = Column(String)
    player_id = Column(Integer, ForeignKey("player.id"))
    player_team_id = Column(Integer, ForeignKey("team.id"))
    opponent_team_id = Column(Integer, ForeignKey("team.id"))
    boxscore_id = Column(Integer, ForeignKey("boxscore.id"))
    season_id = Column(Integer, ForeignKey("season.id"))

    player_team = relationship(
        "Team", foreign_keys=[player_team_id], back_populates="team_pitching_stats"
    )
    opponent_team = relationship(
        "Team",
        foreign_keys=[opponent_team_id],
        back_populates="opponent_pitching_stats",
    )

    def __repr__(self):
        return f"<GamePitchStats game_id={self.bbref_game_id}, player_id={self.player_id_br}>"

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def display(self):
        pitch_stats_dict = self.as_dict()
        title = f"Pitching stats for player {self.player_id_br}, game: {self.bbref_game_id}"
        display_dict(pitch_stats_dict, title=title)
