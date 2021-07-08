"""Values used to identify a single player in various websites and databases."""
from sqlalchemy import Column, ForeignKey, Integer, String

import vigorish.database as db
from vigorish.util.string_helpers import validate_at_bat_id


class PlayerId(db.Base):
    """Values used to identify a single player in various websites and databases."""

    __tablename__ = "player_id"
    mlb_id = Column(Integer, primary_key=True)
    mlb_name = Column(String)
    bbref_id = Column(String, index=True)
    bbref_name = Column(String)
    db_player_id = Column(Integer, ForeignKey("player.id"))

    def __repr__(self):
        return f"<PlayerId bbref_id={self.bbref_id}, mlb_id={self.mlb_id}>"

    def as_dict(self):
        return {
            "mlb_id": self.mlb_id,
            "mlb_name": self.mlb_name,
            "bbref_id": self.bbref_id,
            "db_player_id": self.db_player_id,
        }

    @classmethod
    def find_by_bbref_id(cls, db_session, bbref_id):
        return db_session.query(cls).filter_by(bbref_id=bbref_id).first()

    @classmethod
    def find_by_mlb_id(cls, db_session, mlb_id) -> "PlayerId":
        return db_session.query(cls).filter_by(mlb_id=mlb_id).first()

    @classmethod
    def get_player_ids_from_at_bat_id(cls, db_session, at_bat_id):
        at_bat_dict = validate_at_bat_id(at_bat_id).value
        pitcher_id_mlb = int(at_bat_dict["pitcher_mlb_id"])
        batter_id_mlb = int(at_bat_dict["batter_mlb_id"])
        pitcher_id = cls.find_by_mlb_id(db_session, pitcher_id_mlb)
        batter_id = cls.find_by_mlb_id(db_session, batter_id_mlb)
        return {
            "pitcher_id_bbref": pitcher_id.bbref_id,
            "pitcher_id_mlb": pitcher_id_mlb,
            "pitcher_id_db": pitcher_id.db_player_id,
            "pitcher_name": pitcher_id.mlb_name,
            "pitcher_team": at_bat_dict["pitcher_team"],
            "batter_id_bbref": batter_id.bbref_id,
            "batter_id_mlb": batter_id_mlb,
            "batter_id_db": batter_id.db_player_id,
            "batter_name": batter_id.mlb_name,
            "batter_team": at_bat_dict["batter_team"],
        }

    @classmethod
    def get_player_id_map(cls, db_session):
        all_players = db_session.query(cls).all()
        return {p.mlb_id: p.db_player_id for p in all_players}
