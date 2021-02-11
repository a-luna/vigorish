"""Association table linking Players to Teams."""
from sqlalchemy import Column, ForeignKey, Integer, String

import vigorish.database as db


class Assoc_Player_Team(db.Base):
    __tablename__ = "player_team"

    db_player_id = Column(Integer, ForeignKey("player.id"), primary_key=True)
    db_team_id = Column(Integer, ForeignKey("team.id"), primary_key=True)
    mlb_id = Column(Integer)
    bbref_id = Column(String)
    team_id = Column(String)
    year = Column(Integer)
    stint_number = Column(Integer)
    season_id = Column(Integer, ForeignKey("season.id"))
