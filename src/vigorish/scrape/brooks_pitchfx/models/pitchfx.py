"""Pitchfx measurements for a single pitch scraped from brooksbaseball.com."""
from dataclasses import dataclass, field
from datetime import datetime, timezone

from vigorish.util.datetime_util import TIME_ZONE_NEW_YORK
from vigorish.util.dt_format_strings import DT_AWARE
from vigorish.util.regex import PFX_TIMESTAMP_REGEX
from vigorish.util.string_helpers import validate_bbref_game_id


@dataclass
class BrooksPitchFxData:
    """Pitchfx measurements for a single pitch scraped from brooksbaseball.com."""

    pitcher_name: str = field(repr=False, default=None)
    pitch_app_id: str = None
    pitcher_id: int = field(repr=False, default=0)
    batter_id: int = field(repr=False, default=0)
    pitcher_team_id_bb: str = field(repr=False, default=None)
    opponent_team_id_bb: str = field(repr=False, default=None)
    bb_game_id: str = field(repr=False, default=None)
    bbref_game_id: str = field(repr=False, default=None)
    table_row_number: int = field(repr=False, default=0)
    park_sv_id: str = field(repr=False, default=None)
    play_guid: str = field(repr=False, default=None)
    ab_total: int = field(repr=False, default=0)
    ab_count: int = field(repr=False, default=0)
    ab_id: int = field(repr=False, default=0)
    des: str = field(repr=False, default=None)
    type: str = field(repr=False, default=None)
    id: int = 0
    sz_top: float = field(repr=False, default=0.0)
    sz_bot: float = field(repr=False, default=0.0)
    pfx_xdatafile: float = field(repr=False, default=0.0)
    pfx_zdatafile: float = field(repr=False, default=0.0)
    mlbam_pitch_name: str = None
    zone_location: int = field(repr=False, default=0)
    pitch_con: float = field(repr=False, default=0.0)
    stand: str = field(repr=False, default=None)
    strikes: int = field(repr=False, default=0)
    balls: int = field(repr=False, default=0)
    p_throws: str = field(repr=False, default=None)
    pdes: str = field(repr=False, default=None)
    spin: float = field(repr=False, default=0.0)
    norm_ht: float = field(repr=False, default=0.0)
    inning: float = field(repr=False, default=0.0)
    tstart: float = field(repr=False, default=0.0)
    vystart: float = field(repr=False, default=0.0)
    ftime: float = field(repr=False, default=0.0)
    pfx_x: float = field(repr=False, default=0.0)
    pfx_z: float = field(repr=False, default=0.0)
    uncorrected_pfx_x: float = field(repr=False, default=0.0)
    uncorrected_pfx_z: float = field(repr=False, default=0.0)
    x0: float = field(repr=False, default=0.0)
    y0: float = field(repr=False, default=0.0)
    z0: float = field(repr=False, default=0.0)
    vx0: float = field(repr=False, default=0.0)
    vy0: float = field(repr=False, default=0.0)
    vz0: float = field(repr=False, default=0.0)
    ax: float = field(repr=False, default=0.0)
    ay: float = field(repr=False, default=0.0)
    az: float = field(repr=False, default=0.0)
    start_speed: float = field(repr=False, default=0.0)
    px: float = field(repr=False, default=0.0)
    pz: float = field(repr=False, default=0.0)
    pxold: float = field(repr=False, default=0.0)
    pzold: float = field(repr=False, default=0.0)
    tm_spin: int = field(repr=False, default=0)
    sb: int = field(repr=False, default=0)
    game_start_time_str: str = field(repr=False, default=None)
    time_pitch_thrown_str: str = field(repr=False, default=None)
    has_zone_location: bool = field(repr=False, default=False)
    batter_did_swing: bool = field(repr=False, default=False)
    batter_made_contact: bool = field(repr=False, default=False)
    called_strike: bool = field(repr=False, default=False)
    swinging_strike: bool = field(repr=False, default=False)
    inside_strike_zone: bool = field(repr=False, default=False)
    outside_strike_zone: bool = field(repr=False, default=False)
    swing_inside_zone: bool = field(repr=False, default=False)
    swing_outside_zone: bool = field(repr=False, default=False)
    contact_inside_zone: bool = field(repr=False, default=False)
    contact_outside_zone: bool = field(repr=False, default=False)
    is_batted_ball: bool = field(repr=False, default=False)
    is_ground_ball: bool = field(repr=False, default=False)
    is_fly_ball: bool = field(repr=False, default=False)
    is_line_drive: bool = field(repr=False, default=False)
    is_pop_up: bool = field(repr=False, default=False)
    is_patched: bool = field(repr=False, default=False)
    is_duplicate_guid: bool = field(repr=False, default=False)
    is_duplicate_pitch_number: bool = field(repr=False, default=False)
    is_invalid_ibb: bool = field(repr=False, default=False)
    is_out_of_sequence: bool = field(repr=False, default=False)

    @property
    def game_start_time(self):
        return datetime.strptime(self.game_start_time_str, DT_AWARE) if self.game_start_time_str else None

    @property
    def time_pitch_thrown(self):
        match = PFX_TIMESTAMP_REGEX.match(self.park_sv_id)
        if not match:
            return None
        group_dict = match.groupdict()
        if int(group_dict["second"]) >= 60:
            group_dict["second"] = 0
        game_dict = validate_bbref_game_id(self.bbref_game_id).value
        timestamp = datetime(
            game_dict["game_date"].year,
            int(group_dict["month"]),
            int(group_dict["day"]),
            int(group_dict["hour"]),
            int(group_dict["minute"]),
            int(group_dict["second"]),
        )
        thrown_as_tz = timestamp.replace(tzinfo=timezone.utc).astimezone(TIME_ZONE_NEW_YORK)
        thrown_replace_tz = timestamp.replace(tzinfo=TIME_ZONE_NEW_YORK)
        return (
            thrown_as_tz
            if self.game_start_time and self.game_start_time < thrown_as_tz
            else thrown_replace_tz
            if self.game_start_time and self.game_start_time < thrown_replace_tz
            else None
        )

    @property
    def seconds_since_game_start(self):
        if not self.time_pitch_thrown or not self.game_start_time:
            return 0
        return int((self.time_pitch_thrown - self.game_start_time).total_seconds())

    def as_dict(self):
        """Convert pitch log to a dictionary."""
        return {
            "__brooks_pitchfx_data__": True,
            "pitcher_name": self.pitcher_name,
            "pitch_app_id": self.pitch_app_id,
            "pitcher_id": self.pitcher_id,
            "batter_id": self.batter_id,
            "pitcher_team_id_bb": self.pitcher_team_id_bb,
            "opponent_team_id_bb": self.opponent_team_id_bb,
            "bb_game_id": self.bb_game_id,
            "bbref_game_id": self.bbref_game_id,
            "table_row_number": self.table_row_number,
            "park_sv_id": self.park_sv_id,
            "play_guid": self.play_guid,
            "ab_total": self.ab_total,
            "ab_count": self.ab_count,
            "ab_id": self.ab_id,
            "des": self.des,
            "type": self.type,
            "id": self.id,
            "sz_top": self.sz_top,
            "sz_bot": self.sz_bot,
            "pfx_xdatafile": self.pfx_xdatafile,
            "pfx_zdatafile": self.pfx_zdatafile,
            "mlbam_pitch_name": self.mlbam_pitch_name,
            "zone_location": self.zone_location,
            "pitch_con": self.pitch_con,
            "stand": self.stand,
            "strikes": self.strikes,
            "balls": self.balls,
            "p_throws": self.p_throws,
            "pdes": self.pdes,
            "spin": self.spin,
            "norm_ht": self.norm_ht,
            "inning": self.inning,
            "tstart": self.tstart,
            "vystart": self.vystart,
            "ftime": self.ftime,
            "pfx_x": self.pfx_x,
            "pfx_z": self.pfx_z,
            "uncorrected_pfx_x": self.uncorrected_pfx_x,
            "uncorrected_pfx_z": self.uncorrected_pfx_z,
            "x0": self.x0,
            "y0": self.y0,
            "z0": self.z0,
            "vx0": self.vx0,
            "vy0": self.vy0,
            "vz0": self.vz0,
            "ax": self.ax,
            "ay": self.ay,
            "az": self.az,
            "start_speed": self.start_speed,
            "px": self.px,
            "pz": self.pz,
            "pxold": self.pxold,
            "pzold": self.pzold,
            "tm_spin": self.tm_spin,
            "sb": self.sb,
            "game_start_time_str": self.game_start_time_str,
            "time_pitch_thrown_str": self.get_time_pitch_thrown_str(),
            "has_zone_location": self.has_zone_location,
            "batter_did_swing": self.batter_did_swing,
            "batter_made_contact": self.batter_made_contact,
            "called_strike": self.called_strike,
            "swinging_strike": self.swinging_strike,
            "inside_strike_zone": self.inside_strike_zone,
            "outside_strike_zone": self.outside_strike_zone,
            "swing_inside_zone": self.swing_inside_zone,
            "swing_outside_zone": self.swing_outside_zone,
            "contact_inside_zone": self.contact_inside_zone,
            "contact_outside_zone": self.contact_outside_zone,
            "is_batted_ball": self.is_batted_ball,
            "is_ground_ball": self.is_ground_ball,
            "is_fly_ball": self.is_fly_ball,
            "is_line_drive": self.is_line_drive,
            "is_pop_up": self.is_pop_up,
            "is_patched": self.is_patched,
            "is_duplicate_guid": self.is_duplicate_guid,
            "is_duplicate_pitch_number": self.is_duplicate_pitch_number,
            "is_invalid_ibb": self.is_invalid_ibb,
            "is_out_of_sequence": self.is_out_of_sequence,
        }

    def get_time_pitch_thrown_str(self):
        return self.time_pitch_thrown.strftime(DT_AWARE) if self.time_pitch_thrown else None
