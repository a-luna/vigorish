"""Association table linking Players to Teams."""
from sqlalchemy import Column, ForeignKey, Integer, String

import vigorish.database as db
from vigorish.constants import TEAM_ID_MAP


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
        metrics = [data.split(",") for data in self.def_pos_list.split("/")] if self.def_pos_list else []
        return [{"def_pos": d[0], "percent": int(d[1])} for d in metrics if len(d) == 2] if metrics else []

    @property
    def bat_order_metrics(self):
        metrics = [data.split(",") for data in self.bat_order_list.split("/")] if self.bat_order_list else []
        return [{"bat_order": int(d[0]), "percent": int(d[1])} for d in metrics if len(d) == 2] if metrics else []

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

    def as_player_team_map(self, db_session):
        player_id = db.PlayerId.find_by_mlb_id(db_session, self.mlb_id)
        return {
            "name_common": player_id.mlb_name,
            "age": 0,
            "mlb_ID": str(self.mlb_id),
            "player_ID": self.bbref_id,
            "year_ID": str(self.year),
            "team_ID": self.team_id,
            "stint_ID": str(self.stint_number),
            "lg_ID": TEAM_ID_MAP[self.team_id]["league"],
        }

    @classmethod
    def get_stints_for_year_for_player(cls, db_session, mlb_id, year):
        stints = (
            db_session.query(cls).filter(cls.mlb_id == mlb_id).filter(cls.year == year).order_by(cls.stint_number).all()
        )
        return [s.as_dict() for s in stints]
