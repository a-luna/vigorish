from dataclasses import dataclass
from datetime import datetime

from dataclass_csv import accept_whitespaces
from sqlalchemy import Column, Float, ForeignKey, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

import vigorish.database as db
from vigorish.util.dt_format_strings import DATE_ONLY_TABLE_ID


class BatStats(db.Base):

    __tablename__ = "bat_stats"
    id = Column(Integer, primary_key=True)
    bbref_game_id = Column(String)
    player_id_mlb = Column(Integer)
    player_id_bbref = Column(String)
    player_team_id_bbref = Column(String)
    opponent_team_id_bbref = Column(String)
    is_starter = Column(Integer, default=0)
    bat_order = Column(Integer)
    def_position = Column(String)
    plate_appearances = Column(Integer)
    at_bats = Column(Integer)
    hits = Column(Integer)
    runs_scored = Column(Integer)
    rbis = Column(Integer)
    bases_on_balls = Column(Integer)
    strikeouts = Column(Integer)
    doubles = Column(Integer, default=0)
    triples = Column(Integer, default=0)
    homeruns = Column(Integer, default=0)
    stolen_bases = Column(Integer, default=0)
    caught_stealing = Column(Integer, default=0)
    hit_by_pitch = Column(Integer, default=0)
    intentional_bb = Column(Integer, default=0)
    gdp = Column(Integer, default=0)
    sac_fly = Column(Integer, default=0)
    sac_hit = Column(Integer, default=0)
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

    player_id = Column(Integer, ForeignKey("player.id"), index=True)
    player_team_id = Column(Integer, ForeignKey("team.id"))
    opponent_team_id = Column(Integer, ForeignKey("team.id"))
    game_status_id = Column(Integer, ForeignKey("scrape_status_game.id"))
    date_id = Column(Integer, ForeignKey("scrape_status_date.id"))
    season_id = Column(Integer, ForeignKey("season.id"))

    player = relationship("Player", foreign_keys=[player_id], backref="bat_stats")
    player_team = relationship("Team", foreign_keys=[player_team_id], backref="team_bat_stats")
    opponent_team = relationship("Team", foreign_keys=[opponent_team_id], backref="oppp_bat_stats")
    game = relationship("GameScrapeStatus", foreign_keys=[game_status_id])
    date = relationship("DateScrapeStatus", foreign_keys=[date_id])
    season = relationship("Season", foreign_keys=[season_id])

    @hybrid_property
    def game_date(self):
        return datetime.strptime(str(self.date_id), DATE_ONLY_TABLE_ID)

    @classmethod
    def from_dict(cls, bbref_game_id, bat_stats_dict):
        bbref_data = bat_stats_dict.pop("bbref_data", {})
        bbref_data["bbref_game_id"] = bbref_game_id
        bbref_data["player_id_mlb"] = bat_stats_dict["batter_id_mlb"]
        bbref_data["player_id_bbref"] = bat_stats_dict["batter_id_bbref"]
        bbref_data["player_team_id_bbref"] = bat_stats_dict["batter_team_id_bbref"]
        bbref_data["opponent_team_id_bbref"] = bat_stats_dict["opponent_team_id_bbref"]
        bbref_data["is_starter"] = int(bat_stats_dict["is_starter"])
        bbref_data["bat_order"] = bat_stats_dict["bat_order"]
        bbref_data["def_position"] = bat_stats_dict["def_position"]
        for d in bbref_data.pop("details", []):
            if d["stat"] == "2B":
                bbref_data["doubles"] = d["count"]
            if d["stat"] == "3B":
                bbref_data["triples"] = d["count"]
            if d["stat"] == "HR":
                bbref_data["homeruns"] = d["count"]
            if d["stat"] == "SB":
                bbref_data["stolen_bases"] = d["count"]
            if d["stat"] == "CS":
                bbref_data["caught_stealing"] = d["count"]
            if d["stat"] == "HBP":
                bbref_data["hit_by_pitch"] = d["count"]
            if d["stat"] == "IW":
                bbref_data["intentional_bb"] = d["count"]
            if d["stat"] == "GDP":
                bbref_data["gdp"] = d["count"]
            if d["stat"] == "SF":
                bbref_data["sac_fly"] = d["count"]
            if d["stat"] == "SH":
                bbref_data["sac_hit"] = d["count"]
        return cls(**bbref_data)

    @classmethod
    def get_all_seasons_with_data_for_player(cls, db_session, player_id_mlb):
        return {bs.season for bs in db_session.query(cls).filter_by(player_id_mlb=player_id_mlb).all()}

    @classmethod
    def get_date_intervals_for_player_teams(cls, db_session, player_id_mlb, year):
        player = db.Player.find_by_mlb_id(db_session, player_id_mlb)
        season = db.Season.find_by_year(db_session, year)
        if not player or not season:
            return []
        bat_stats_for_season = (
            db_session.query(cls).filter(cls.player_id == player.id).filter(cls.season_id == season.id).all()
        )
        all_teams = {(bs.player_team_id_bbref, bs.player_team_id) for bs in bat_stats_for_season}
        player_team_info = []
        for team_id_bbref, team_id in all_teams:
            bat_stats_with_team = [bs for bs in bat_stats_for_season if bs.player_team_id_bbref == team_id_bbref]
            min_date = min(bs.game_date for bs in bat_stats_with_team)
            max_date = max(bs.game_date for bs in bat_stats_with_team)
            player_team_info.append(
                {"team_id": team_id_bbref, "db_team_id": team_id, "min_date": min_date, "max_date": max_date}
            )
        player_team_info.sort(key=lambda x: x["min_date"])
        for stint, team_info in enumerate(player_team_info, start=1):
            team_info["stint_number"] = stint
        return player_team_info


@accept_whitespaces
@dataclass
class BatStatsCsvRow:
    id: int = None
    bbref_game_id: str = None
    player_id_mlb: int = None
    player_id_bbref: str = None
    player_team_id_bbref: str = None
    opponent_team_id_bbref: str = None
    is_starter: int = 0
    bat_order: int = 0
    def_position: str = None
    plate_appearances: int = 0
    at_bats: int = 0
    hits: int = 0
    runs_scored: int = 0
    rbis: int = 0
    bases_on_balls: int = 0
    strikeouts: int = 0
    doubles: int = 0
    triples: int = 0
    homeruns: int = 0
    stolen_bases: int = 0
    caught_stealing: int = 0
    hit_by_pitch: int = 0
    intentional_bb: int = 0
    gdp: int = 0
    sac_fly: int = 0
    sac_hit: int = 0
    avg_to_date: float = 0.0
    obp_to_date: float = 0.0
    slg_to_date: float = 0.0
    ops_to_date: float = 0.0
    total_pitches: int = 0
    total_strikes: int = 0
    wpa_bat: float = 0.0
    avg_lvg_index: float = 0.0
    wpa_bat_pos: float = 0.0
    wpa_bat_neg: float = 0.0
    re24_bat: float = 0.0
