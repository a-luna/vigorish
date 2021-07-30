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
    percent_started = Column(Integer)
    bench_player = Column(Integer, default=0)
    percent_bench = Column(Integer)
    starting_pitcher = Column(Integer, default=0)
    percent_sp = Column(Integer)
    relief_pitcher = Column(Integer, default=0)
    percent_rp = Column(Integer)
    def_pos_list = Column(String)
    bat_order_list = Column(String)
    role_is_defined = Column(Integer, default=0)

    season_id = Column(Integer, ForeignKey("season.id"))

    @property
    def player_team_role(self):
        if bool(self.starting_pitcher) or bool(self.relief_pitcher):
            return "pitching"
        return "batting"

    @property
    def def_pos_metrics(self):
        metrics = [data.split(",") for data in self.def_pos_list.split("/")]
        return [{"def_pos": d[0], "percent": int(d[1])} for d in metrics if len(d) == 2]

    @property
    def bat_order_metrics(self):
        metrics = [data.split(",") for data in self.bat_order_list.split("/")]
        return [{"bat_order": int(d[0]), "percent": int(d[1])} for d in metrics if len(d) == 2]

    def as_dict(self):
        return {
            "mlb_id": self.mlb_id,
            "bbref_id": self.bbref_id,
            "team_id": self.team_id,
            "year": self.year,
            "role": self.player_team_role,
            "stint_number": self.stint_number,
            "starting_lineup": bool(self.starting_lineup),
            "percent_started": self.percent_started,
            "bench_player": bool(self.bench_player),
            "percent_bench": self.percent_bench,
            "starting_pitcher": bool(self.starting_pitcher),
            "percent_sp": self.percent_sp,
            "relief_pitcher": bool(self.relief_pitcher),
            "percent_rp": self.percent_rp,
            "def_pos_list": self.def_pos_metrics,
            "bat_order_list": self.bat_order_metrics,
        }
