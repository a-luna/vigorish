from dataclasses import dataclass
from datetime import datetime, timezone

from dataclass_csv import accept_whitespaces, dateformat
from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from vigorish.config.database import Base
from vigorish.util.dataclass_helpers import dict_from_dataclass, sanitize_row_dict
from vigorish.util.datetime_util import make_tzaware, TIME_ZONE_NEW_YORK
from vigorish.util.dt_format_strings import CSV_UTC, DT_AWARE
from vigorish.util.string_helpers import csv_sanitize


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
    game_start_time_utc = Column(DateTime)
    time_pitch_thrown_utc = Column(DateTime)
    seconds_since_game_start = Column(Integer)
    has_zone_location = Column(Integer)
    table_row_number = Column(Integer)
    is_patched = Column(Integer)
    is_duplicate_guid = Column(Integer)
    is_duplicate_pitch_number = Column(Integer)
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

    pitcher = relationship("Player", foreign_keys=[pitcher_id])
    batter = relationship("Player", foreign_keys=[batter_id])
    team_pitching = relationship("Team", foreign_keys=[team_pitching_id])
    team_batting = relationship("Team", foreign_keys=[team_batting_id])
    game = relationship("GameScrapeStatus", foreign_keys=[game_status_id])
    pitch_app = relationship("PitchAppScrapeStatus", foreign_keys=[pitch_app_db_id])
    date = relationship("DateScrapeStatus", foreign_keys=[date_id])
    season = relationship("Season", foreign_keys=[season_id])

    @hybrid_property
    def game_date(self):
        return self.game_start_time.date()

    @hybrid_property
    def game_start_time(self):
        game_start_utc = make_tzaware(self.game_start_time_utc, use_tz=timezone.utc, localize=False)
        return make_tzaware(game_start_utc, use_tz=TIME_ZONE_NEW_YORK, localize=True)

    @hybrid_property
    def time_pitch_thrown(self):
        thrown_utc = make_tzaware(self.time_pitch_thrown_utc, use_tz=timezone.utc, localize=False)
        return make_tzaware(thrown_utc, use_tz=TIME_ZONE_NEW_YORK, localize=True)

    def as_csv_dict(self):
        self.pdes = csv_sanitize(self.pdes)
        return dict_from_dataclass(self, PitchFxCsvRow, date_format=CSV_UTC)

    @classmethod
    def get_csv_col_names(cls):
        return [name for name in PitchFxCsvRow.__dataclass_fields__.keys()]

    @classmethod
    def export_table_as_csv(cls, db_session):
        col_names = ",".join(cls.get_csv_col_names())
        csv_dicts = (obj.as_csv_dict() for obj in db_session.query(cls).all())
        csv_rows = (",".join(sanitize_row_dict(d, date_format=CSV_UTC)) for d in csv_dicts)
        yield col_names
        for row in csv_rows:
            yield row

    @classmethod
    def from_dict(cls, pfx_dict):
        game_start_str = pfx_dict.pop("game_start_time_str")
        pitch_thrown_str = pfx_dict.pop("time_pitch_thrown_str")
        game_start_time = datetime.strptime(game_start_str, DT_AWARE).astimezone(timezone.utc)
        time_pitch_thrown = datetime.strptime(pitch_thrown_str, DT_AWARE).astimezone(timezone.utc)
        seconds_since_game_start = int((time_pitch_thrown - game_start_time).total_seconds())
        pfx_dict["game_start_time_utc"] = game_start_time
        pfx_dict["time_pitch_thrown_utc"] = time_pitch_thrown
        pfx_dict["seconds_since_game_start"] = seconds_since_game_start
        pfx_dict["basic_type"] = pfx_dict.pop("type")
        pfx_dict["pitch_id"] = pfx_dict.pop("id")
        pfx_dict["pitcher_id_mlb"] = int(pfx_dict.pop("pitcher_id"))
        pfx_dict["batter_id_mlb"] = int(pfx_dict.pop("batter_id"))
        pfx_dict["zone_location"] = int(pfx_dict["zone_location"])
        pfx_dict["spin"] = round(pfx_dict["spin"], 1)
        pfx_dict["pfx_x"] = round(pfx_dict["pfx_x"], 2)
        pfx_dict["pfx_z"] = round(pfx_dict["pfx_z"], 2)
        pfx_dict["px"] = round(pfx_dict["px"], 2)
        pfx_dict["pz"] = round(pfx_dict["pz"], 2)
        pfx_dict.pop("play_guid", None)
        pfx_dict.pop("pitcher_name", None)
        pfx_dict.pop("pitch_con", None)
        pfx_dict.pop("norm_ht", None)
        pfx_dict.pop("tstart", None)
        pfx_dict.pop("vystart", None)
        pfx_dict.pop("ftime", None)
        pfx_dict.pop("x0", None)
        pfx_dict.pop("y0", None)
        pfx_dict.pop("z0", None)
        pfx_dict.pop("vx0", None)
        pfx_dict.pop("vy0", None)
        pfx_dict.pop("vz0", None)
        pfx_dict.pop("ax", None)
        pfx_dict.pop("ay", None)
        pfx_dict.pop("az", None)
        pfx_dict.pop("tm_spin", None)
        pfx_dict.pop("sb", None)
        return cls(**pfx_dict)


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
    spin: float = None
    zone_location: int = None
    sz_top: float = None
    sz_bot: float = None
    pfx_xdatafile: float = None
    pfx_zdatafile: float = None
    pfx_x: float = None
    pfx_z: float = None
    uncorrected_pfx_x: float = None
    uncorrected_pfx_z: float = None
    px: float = None
    pz: float = None
    pxold: float = None
    pzold: float = None
    park_sv_id: str = None
    game_start_time_utc: datetime = None
    time_pitch_thrown_utc: datetime = None
    seconds_since_game_start: int = 0
    has_zone_location: int = 0
    table_row_number: int = 0
    is_patched: int = 0
    is_duplicate_guid: int = 0
    is_duplicate_pitch_number: int = 0
    is_invalid_ibb: int = 0
    is_out_of_sequence: int = 0
