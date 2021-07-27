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
    starting_lineup = Column(Integer, default=0)
    bench_player = Column(Integer, default=0)
    starting_pitcher = Column(Integer, default=0)
    relief_pitcher = Column(Integer, default=0)
    def_pos_list = Column(String)
    role_is_defined = Column(Integer, default=0)

    season_id = Column(Integer, ForeignKey("season.id"))

    def as_dict(self):
        return {
            "mlb_id": self.mlb_id,
            "bbref_id": self.bbref_id,
            "team_id": self.team_id,
            "year": self.year,
            "stint_number": self.stint_number,
            "starting_lineup": bool(self.starting_lineup),
            "bench_player": bool(self.bench_player),
            "starting_pitcher": bool(self.starting_pitcher),
            "relief_pitcher": bool(self.relief_pitcher),
            "def_pos_list": self.def_pos_list,
        }
