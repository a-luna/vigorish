"""Total batting statistics for a single player in a single game."""
from sqlalchemy import Column, Integer, Float, String, ForeignKey
from sqlalchemy.orm import relationship

from vigorish.config.database import Base
from vigorish.util.list_helpers import display_dict


class GameBatStats(Base):
    """Total batting statistics for a single player in a single game."""

    __tablename__ = "game_bat_stats"
    id = Column(Integer, primary_key=True)
    at_bats = Column(Integer)
    runs_scored = Column(Integer)
    hits = Column(Integer)
    rbis = Column(Integer)
    bases_on_balls = Column(Integer)
    strikeouts = Column(Integer)
    plate_appearances = Column(Integer)
    doubles = Column(Integer)
    triples = Column(Integer)
    homeruns = Column(Integer)
    sacrifice_hits = Column(Integer)
    sacrifice_flies = Column(Integer)
    intentional_walks = Column(Integer)
    hit_by_pitch = Column(Integer)
    ground_into_double_play = Column(Integer)
    stolen_bases = Column(Integer)
    caught_stealing = Column(Integer)
    avg_to_date = Column(Float)
    obp_to_date = Column(Float)
    slg_to_date = Column(Float)
    ops_to_date = Column(Float)
    total_pitches = Column(Integer)
    total_strikes = Column(Integer)
    wpa_bat = Column(Float)
    avg_lvg_index = Column(Float)
    wpa_bat_pos = Column(Float)
    wpa_bat_neg = Column(Float)
    re24_bat = Column(Float)
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
        "Team", foreign_keys=[player_team_id], back_populates="team_batting_stats"
    )
    opponent_team = relationship(
        "Team", foreign_keys=[opponent_team_id], back_populates="opponent_batting_stats"
    )

    def __repr__(self):
        return (
            f"<GameBatStats bbref_game_id={self.bbref_game_id}, player_id_br={self.player_id_br}>"
        )

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def display(self):
        bat_stats_dict = self.as_dict()
        title = f"Batting stats for player {self.player_id_br}, game: {self.bbref_game_id}"
        display_dict(bat_stats_dict, title=title)
