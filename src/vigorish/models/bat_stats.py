from dataclasses import dataclass

from dataclass_csv import accept_whitespaces
from sqlalchemy import Column, Float, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from vigorish.database import Base


class BatStats(Base):

    __tablename__ = "bat_stats"
    id = Column(Integer, primary_key=True)
    bbref_game_id = Column(String)
    player_id_mlb = Column(Integer)
    player_id_bbref = Column(String)
    player_team_id_bbref = Column(String)
    opponent_team_id_bbref = Column(String)
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
    player_team = relationship("Team", foreign_keys=[player_team_id])
    opponent_team = relationship("Team", foreign_keys=[opponent_team_id])
    game = relationship("GameScrapeStatus", foreign_keys=[game_status_id])
    date = relationship("DateScrapeStatus", foreign_keys=[date_id])
    season = relationship("Season", foreign_keys=[season_id])

    @classmethod
    def from_dict(cls, bbref_game_id, bat_stats_dict):
        bbref_data = bat_stats_dict.pop("bbref_data", {})
        bbref_data["bbref_game_id"] = bbref_game_id
        bbref_data["player_id_mlb"] = bat_stats_dict["batter_id_mlb"]
        bbref_data["player_id_bbref"] = bat_stats_dict["batter_id_bbref"]
        bbref_data["player_team_id_bbref"] = bat_stats_dict["batter_team_id_bbref"]
        bbref_data["opponent_team_id_bbref"] = bat_stats_dict["opponent_team_id_bbref"]
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


@accept_whitespaces
@dataclass
class BatStatsCsvRow:
    id: int = None
    bbref_game_id: str = None
    player_id_mlb: int = None
    player_id_bbref: str = None
    player_team_id_bbref: str = None
    opponent_team_id_bbref: str = None
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
