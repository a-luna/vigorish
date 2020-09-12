from sqlalchemy import Column, Integer, String, DateTime, Boolean, Float

# ForeignKey
# from sqlalchemy.ext.hybrid import hybrid_property

from vigorish.config.database import Base


class PitchFx(Base):

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
    spin = Column(Float)
    zone_location = Column(Integer)
    sz_top = Column(Float)
    sz_bot = Column(Float)
    pfx_xdatafile = Column(Float)
    pfx_zdatafile = Column(Float)
    pfx_x = Column(Float)
    pfx_z = Column(Float)
    uncorrected_pfx_x = Column(Float)
    uncorrected_pfx_z = Column(Float)
    px = Column(Float)
    pz = Column(Float)
    pxold = Column(Float)
    pzold = Column(Float)
    park_sv_id = Column(String)
    game_start_time = Column(DateTime)
    time_pitch_thrown_str = Column(DateTime)
    seconds_since_game_start = Column(Integer)
    has_zone_location = Column(Boolean)
    table_row_number = Column(Integer)
    is_patched = Column(Boolean)
