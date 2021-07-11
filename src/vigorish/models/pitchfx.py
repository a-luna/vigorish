from dataclasses import dataclass
from datetime import datetime, timezone

from dataclass_csv import accept_whitespaces, dateformat
from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

import vigorish.database as db
from vigorish.util.datetime_util import make_tzaware, TIME_ZONE_NEW_YORK
from vigorish.util.dt_format_strings import CSV_UTC, DT_AWARE


class PitchFx(db.Base):

    __tablename__ = "pitchfx"
    id = Column(Integer, primary_key=True)
    bb_game_id = Column(String)
    bbref_game_id = Column(String)
    pitch_app_id = Column(String)
    inning_id = Column(String)
    at_bat_id = Column(String)
    pitcher_id_mlb = Column(Integer)
    batter_id_mlb = Column(Integer)
    pitcher_id_bbref = Column(String)
    batter_id_bbref = Column(String)
    pitcher_team_id_bb = Column(String)
    opponent_team_id_bb = Column(String)
    p_throws = Column(String)
    stand = Column(String)
    pitch_id = Column(Integer)
    inning = Column(Integer)
    ab_total = Column(Integer)
    ab_count = Column(Integer)
    ab_id = Column(Integer)
    des = Column(String)
    strikes = Column(Integer)
    balls = Column(Integer)
    basic_type = Column(String)
    pdes = Column(String)
    mlbam_pitch_name = Column(String)
    start_speed = Column(Float)
    zone_location = Column(Integer)
    sz_top = Column(Float)
    sz_bot = Column(Float)
    pfx_x = Column(Float)
    pfx_z = Column(Float)
    x0 = Column(Float)
    y0 = Column(Float)
    z0 = Column(Float)
    vx0 = Column(Float)
    vy0 = Column(Float)
    vz0 = Column(Float)
    ax = Column(Float)
    ay = Column(Float)
    az = Column(Float)
    px = Column(Float)
    pz = Column(Float)
    park_sv_id = Column(String)
    plate_time = Column(Float)
    extension = Column(Float)
    break_angle = Column(Float)
    break_length = Column(Float)
    break_y = Column(Float)
    spin_rate = Column(Integer)
    spin_direction = Column(Integer)
    launch_speed = Column(Float)
    launch_angle = Column(Float)
    total_distance = Column(Float)
    trajectory = Column(String)
    hardness = Column(String)
    location = Column(Integer)
    coord_x = Column(Float)
    coord_y = Column(Float)
    game_start_time_utc = Column(DateTime)
    time_pitch_thrown_utc = Column(DateTime)
    seconds_since_game_start = Column(Integer)
    has_zone_location = Column(Integer)
    batter_did_swing = Column(Integer)
    batter_made_contact = Column(Integer)
    called_strike = Column(Integer)
    swinging_strike = Column(Integer)
    inside_strike_zone = Column(Integer)
    outside_strike_zone = Column(Integer)
    swing_inside_zone = Column(Integer)
    swing_outside_zone = Column(Integer)
    contact_inside_zone = Column(Integer)
    contact_outside_zone = Column(Integer)
    is_in_play = Column(Integer)
    is_ground_ball = Column(Integer)
    is_fly_ball = Column(Integer)
    is_line_drive = Column(Integer)
    is_popup = Column(Integer)
    is_hard_hit = Column(Integer)
    is_medium_hit = Column(Integer)
    is_soft_hit = Column(Integer)
    is_barreled = Column(Integer)
    is_final_pitch_of_ab = Column(Integer)
    ab_result_out = Column(Integer)
    ab_result_hit = Column(Integer)
    ab_result_single = Column(Integer)
    ab_result_double = Column(Integer)
    ab_result_triple = Column(Integer)
    ab_result_homerun = Column(Integer)
    ab_result_bb = Column(Integer)
    ab_result_ibb = Column(Integer)
    ab_result_k = Column(Integer)
    ab_result_hbp = Column(Integer)
    ab_result_error = Column(Integer)
    ab_result_sac_hit = Column(Integer)
    ab_result_sac_fly = Column(Integer)
    ab_result_unclear = Column(Integer)
    pitch_type_int = Column(Integer)
    pbp_play_result = Column(String)
    pbp_runs_outs_result = Column(String)
    is_sp = Column(Integer)
    is_rp = Column(Integer)
    is_patched = Column(Integer)
    is_invalid_ibb = Column(Integer)
    is_out_of_sequence = Column(Integer)

    pitcher_id = Column(Integer, ForeignKey("player.id"), index=True)
    batter_id = Column(Integer, ForeignKey("player.id"), index=True)
    team_pitching_id = Column(Integer, ForeignKey("team.id"))
    team_batting_id = Column(Integer, ForeignKey("team.id"))
    game_status_id = Column(Integer, ForeignKey("scrape_status_game.id"))
    pitch_app_db_id = Column(Integer, ForeignKey("scrape_status_pitch_app.id"), index=True)
    date_id = Column(Integer, ForeignKey("scrape_status_date.id"))
    season_id = Column(Integer, ForeignKey("season.id"))

    pitcher = relationship("Player", foreign_keys=[pitcher_id], backref="pfx_as_pitcher")
    batter = relationship("Player", foreign_keys=[batter_id], backref="pfx_as_batter")
    team_pitching = relationship("Team", foreign_keys=[team_pitching_id])
    team_batting = relationship("Team", foreign_keys=[team_batting_id])
    game = relationship("GameScrapeStatus", foreign_keys=[game_status_id])
    pitch_app = relationship("PitchAppScrapeStatus", foreign_keys=[pitch_app_db_id])
    date = relationship("DateScrapeStatus", foreign_keys=[date_id])
    season = relationship("Season", foreign_keys=[season_id])

    @hybrid_property
    def game_date(self):
        year = self.game_start_time.year
        month = self.game_start_time.month
        day = self.game_start_time.day
        return datetime(year, month, day)

    @hybrid_property
    def game_start_time(self):
        game_start_utc = make_tzaware(self.game_start_time_utc, use_tz=timezone.utc, localize=False)
        return make_tzaware(game_start_utc, use_tz=TIME_ZONE_NEW_YORK, localize=True)

    @hybrid_property
    def time_pitch_thrown(self):
        thrown_utc = make_tzaware(self.time_pitch_thrown_utc, use_tz=timezone.utc, localize=False)
        return make_tzaware(thrown_utc, use_tz=TIME_ZONE_NEW_YORK, localize=True)

    @classmethod
    def from_dict(cls, pfx_dict):
        pfx_dict = cls.update_pfx_dict(pfx_dict)
        pfx_dict["zone_location"] = int(pfx_dict["zone_location"])
        pfx_dict["batter_did_swing"] = int(pfx_dict["batter_did_swing"])
        pfx_dict["batter_made_contact"] = int(pfx_dict["batter_made_contact"])
        pfx_dict["called_strike"] = int(pfx_dict["called_strike"])
        pfx_dict["swinging_strike"] = int(pfx_dict["swinging_strike"])
        pfx_dict["inside_strike_zone"] = int(pfx_dict["inside_strike_zone"])
        pfx_dict["outside_strike_zone"] = int(pfx_dict["outside_strike_zone"])
        pfx_dict["swing_inside_zone"] = int(pfx_dict["swing_inside_zone"])
        pfx_dict["swing_outside_zone"] = int(pfx_dict["swing_outside_zone"])
        pfx_dict["contact_inside_zone"] = int(pfx_dict["contact_inside_zone"])
        pfx_dict["contact_outside_zone"] = int(pfx_dict["contact_outside_zone"])
        pfx_dict["is_in_play"] = int(pfx_dict["is_in_play"])
        pfx_dict["is_ground_ball"] = int(pfx_dict["is_ground_ball"])
        pfx_dict["is_fly_ball"] = int(pfx_dict["is_fly_ball"])
        pfx_dict["is_line_drive"] = int(pfx_dict["is_line_drive"])
        pfx_dict["is_popup"] = int(pfx_dict["is_popup"])
        pfx_dict["is_hard_hit"] = int(pfx_dict["is_hard_hit"])
        pfx_dict["is_medium_hit"] = int(pfx_dict["is_medium_hit"])
        pfx_dict["is_soft_hit"] = int(pfx_dict["is_soft_hit"])
        pfx_dict["is_barreled"] = int(pfx_dict["is_barreled"])
        pfx_dict["is_final_pitch_of_ab"] = int(pfx_dict["is_final_pitch_of_ab"])
        pfx_dict["ab_result_out"] = int(pfx_dict["ab_result_out"])
        pfx_dict["ab_result_hit"] = int(pfx_dict["ab_result_hit"])
        pfx_dict["ab_result_single"] = int(pfx_dict["ab_result_single"])
        pfx_dict["ab_result_double"] = int(pfx_dict["ab_result_double"])
        pfx_dict["ab_result_triple"] = int(pfx_dict["ab_result_triple"])
        pfx_dict["ab_result_homerun"] = int(pfx_dict["ab_result_homerun"])
        pfx_dict["ab_result_bb"] = int(pfx_dict["ab_result_bb"])
        pfx_dict["ab_result_ibb"] = int(pfx_dict["ab_result_ibb"])
        pfx_dict["ab_result_k"] = int(pfx_dict["ab_result_k"])
        pfx_dict["ab_result_hbp"] = int(pfx_dict["ab_result_hbp"])
        pfx_dict["ab_result_error"] = int(pfx_dict["ab_result_error"])
        pfx_dict["ab_result_sac_hit"] = int(pfx_dict["ab_result_sac_hit"])
        pfx_dict["ab_result_sac_fly"] = int(pfx_dict["ab_result_sac_fly"])
        pfx_dict["ab_result_unclear"] = int(pfx_dict["ab_result_unclear"])
        pfx_dict["is_sp"] = int(pfx_dict["is_sp"])
        pfx_dict["is_rp"] = int(pfx_dict["is_rp"])
        return cls(**pfx_dict)

    @staticmethod
    def update_pfx_dict(pfx_dict):
        game_start_str = pfx_dict.pop("game_start_time_str")
        pitch_thrown_str = pfx_dict.pop("time_pitch_thrown_str")
        game_start_time = datetime.strptime(game_start_str, DT_AWARE).astimezone(timezone.utc)
        time_pitch_thrown = (
            datetime.strptime(pitch_thrown_str, DT_AWARE).astimezone(timezone.utc) if pitch_thrown_str else None
        )
        seconds_since_game_start = (
            int((time_pitch_thrown - game_start_time).total_seconds()) if time_pitch_thrown else 0
        )
        pfx_dict["game_start_time_utc"] = game_start_time
        pfx_dict["time_pitch_thrown_utc"] = time_pitch_thrown
        pfx_dict["seconds_since_game_start"] = seconds_since_game_start
        pfx_dict["basic_type"] = pfx_dict.pop("type")
        pfx_dict["pitch_id"] = pfx_dict.pop("id")
        pfx_dict["pitcher_id_mlb"] = int(pfx_dict.pop("pitcher_id"))
        pfx_dict["batter_id_mlb"] = int(pfx_dict.pop("batter_id"))
        pfx_dict["pitcher_id"] = pfx_dict.pop("pitcher_id_db")
        pfx_dict["batter_id"] = pfx_dict.pop("batter_id_db")
        pfx_dict.pop("play_guid", None)
        pfx_dict.pop("pitcher_name", None)
        return pfx_dict


@accept_whitespaces
@dateformat(CSV_UTC)
@dataclass
class PitchFxCsvRow:
    id: int = None
    bb_game_id: str = None
    bbref_game_id: str = None
    pitch_app_id: str = None
    inning_id: str = None
    at_bat_id: str = None
    pitcher_id_mlb: int = None
    batter_id_mlb: int = None
    pitcher_id_bbref: str = None
    batter_id_bbref: str = None
    pitcher_team_id_bb: str = None
    opponent_team_id_bb: str = None
    p_throws: str = None
    stand: str = None
    pitch_id: int = None
    inning: int = None
    ab_total: int = None
    ab_count: int = None
    ab_id: int = None
    des: str = None
    strikes: int = 0
    balls: int = 0
    basic_type: str = None
    pdes: str = None
    mlbam_pitch_name: str = None
    start_speed: float = None
    zone_location: int = None
    sz_top: float = None
    sz_bot: float = None
    pfx_x: float = None
    pfx_z: float = None
    x0: float = None
    y0: float = None
    z0: float = None
    vx0: float = None
    vy0: float = None
    vz0: float = None
    ax: float = None
    ay: float = None
    az: float = None
    px: float = None
    pz: float = None
    px: float = None
    pz: float = None
    pxold: float = None
    pzold: float = None
    park_sv_id: str = None
    plate_time: float = None
    extension: float = None
    break_angle: float = None
    break_length: float = None
    break_y: float = None
    spin_rate: int = 0
    spin_direction: int = 0
    launch_speed: float = None
    launch_angle: float = None
    total_distance: float = None
    trajectory: str = None
    hardness: str = None
    location: int = 0
    coord_x: float = None
    coord_y: float = None
    game_start_time_utc: datetime = None
    time_pitch_thrown_utc: datetime = None
    seconds_since_game_start: int = 0
    has_zone_location: int = 0
    batter_did_swing: int = 0
    batter_made_contact: int = 0
    called_strike: int = 0
    swinging_strike: int = 0
    inside_strike_zone: int = 0
    outside_strike_zone: int = 0
    swing_inside_zone: int = 0
    swing_outside_zone: int = 0
    contact_inside_zone: int = 0
    contact_outside_zone: int = 0
    is_in_play: int = 0
    is_ground_ball: int = 0
    is_fly_ball: int = 0
    is_line_drive: int = 0
    is_popup: int = 0
    is_hard_hit: int = 0
    is_medium_hit: int = 0
    is_soft_hit: int = 0
    is_barreled: int = 0
    is_final_pitch_of_ab: int = 0
    ab_result_out: int = 0
    ab_result_hit: int = 0
    ab_result_single: int = 0
    ab_result_double: int = 0
    ab_result_triple: int = 0
    ab_result_homerun: int = 0
    ab_result_bb: int = 0
    ab_result_ibb: int = 0
    ab_result_k: int = 0
    ab_result_hbp: int = 0
    ab_result_error: int = 0
    ab_result_sac_hit: int = 0
    ab_result_sac_fly: int = 0
    ab_result_unclear: int = 0
    pitch_type_int: int = 0
    pbp_play_result: str = ""
    pbp_runs_outs_result: str = ""
    is_sp: int = 0
    is_rp: int = 0
    is_patched: int = 0
    is_invalid_ibb: int = 0
    is_out_of_sequence: int = 0
