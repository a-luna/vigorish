from dataclasses import dataclass

from dataclass_csv import accept_whitespaces
from sqlalchemy import Column, Float, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

import vigorish.database as db


class PitchStats(db.Base):

    __tablename__ = "pitch_stats"
    id = Column(Integer, primary_key=True)
    bbref_game_id = Column(String)
    player_id_mlb = Column(Integer)
    player_id_bbref = Column(String)
    player_team_id_bbref = Column(String)
    opponent_team_id_bbref = Column(String)
    is_sp = Column(Integer, default=0)
    is_rp = Column(Integer, default=0)
    is_wp = Column(Integer, default=0)
    is_lp = Column(Integer, default=0)
    is_sv = Column(Integer, default=0)
    innings_pitched = Column(Float)
    total_outs = Column(Integer)
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
    pitch_app_id = Column(String)

    player_id = Column(Integer, ForeignKey("player.id"), index=True)
    player_team_id = Column(Integer, ForeignKey("team.id"))
    opponent_team_id = Column(Integer, ForeignKey("team.id"))
    pitch_app_db_id = Column(Integer, ForeignKey("scrape_status_pitch_app.id"), index=True)
    game_status_id = Column(Integer, ForeignKey("scrape_status_game.id"))
    date_id = Column(Integer, ForeignKey("scrape_status_date.id"))
    season_id = Column(Integer, ForeignKey("season.id"))

    player = relationship("Player", foreign_keys=[player_id], backref="pitch_stats")
    player_team = relationship("Team", foreign_keys=[player_team_id], backref="team_pitch_stats")
    opponent_team = relationship("Team", foreign_keys=[opponent_team_id], backref="opp_pitch_stats")
    game = relationship("GameScrapeStatus", foreign_keys=[game_status_id])
    date = relationship("DateScrapeStatus", foreign_keys=[date_id])
    season = relationship("Season", foreign_keys=[season_id])

    @classmethod
    def from_dict(cls, bbref_game_id, pitch_stats_dict):
        bbref_data = pitch_stats_dict.pop("bbref_data", {})
        bbref_data["bbref_game_id"] = bbref_game_id
        bbref_data["player_id_mlb"] = pitch_stats_dict["pitcher_id_mlb"]
        bbref_data["pitch_app_id"] = f'{bbref_game_id}_{pitch_stats_dict["pitcher_id_mlb"]}'
        bbref_data["player_id_bbref"] = pitch_stats_dict["pitcher_id_bbref"]
        bbref_data["player_team_id_bbref"] = pitch_stats_dict["pitcher_team_id_bbref"]
        bbref_data["opponent_team_id_bbref"] = pitch_stats_dict["opponent_team_id_bbref"]
        bbref_data["is_sp"] = int(pitch_stats_dict["is_sp"])
        bbref_data["is_rp"] = int(pitch_stats_dict["is_rp"])
        bbref_data["is_wp"] = int(pitch_stats_dict["is_wp"])
        bbref_data["is_lp"] = int(pitch_stats_dict["is_lp"])
        bbref_data["is_sv"] = int(pitch_stats_dict["is_sv"])
        bbref_data["total_outs"] = calculate_total_outs(bbref_data["innings_pitched"])
        return cls(**bbref_data)

    @classmethod
    def get_all_seasons_with_data_for_player(cls, db_session, player_id_mlb):
        return {ps.season for ps in db_session.query(cls).filter_by(player_id_mlb=player_id_mlb).all()}


def calculate_total_outs(innings_pitched):
    split = str(innings_pitched).split(".")
    if len(split) != 2:
        return 0
    inn_full = split[0]
    inn_partial = split[1]
    return int(inn_full) * 3 + int(inn_partial)


@accept_whitespaces
@dataclass
class PitchStatsCsvRow:
    id: int = None
    bbref_game_id: str = None
    player_id_mlb: int = None
    player_id_bbref: str = None
    player_team_id_bbref: str = None
    opponent_team_id_bbref: str = None
    is_sp: int = 0
    is_rp: int = 0
    is_wp: int = 0
    is_lp: int = 0
    is_sv: int = 0
    innings_pitched: float = 0.0
    total_outs: int = 0
    hits: int = 0
    runs: int = 0
    earned_runs: int = 0
    bases_on_balls: int = 0
    strikeouts: int = 0
    homeruns: int = 0
    batters_faced: int = 0
    pitch_count: int = 0
    strikes: int = 0
    strikes_contact: int = 0
    strikes_swinging: int = 0
    strikes_looking: int = 0
    ground_balls: int = 0
    fly_balls: int = 0
    line_drives: int = 0
    unknown_type: int = 0
    game_score: int = 0
    inherited_runners: int = 0
    inherited_scored: int = 0
    wpa_pitch: float = 0.0
    avg_lvg_index: float = 0.0
    re24_pitch: float = 0.0
    pitch_app_id: str = None
