"""Pitchfx measurements for a single pitch scraped from brooksbaseball.com."""
from dataclasses import dataclass, field
from datetime import datetime, timezone

from vigorish.enums import PitchType
from vigorish.util.datetime_util import TIME_ZONE_NEW_YORK
from vigorish.util.dt_format_strings import DT_AWARE
from vigorish.util.regex import PFX_TIMESTAMP_REGEX
from vigorish.util.string_helpers import validate_bbref_game_id


@dataclass
class BrooksPitchFxData:
    """Pitchfx measurements for a single pitch scraped from brooksbaseball.com."""

    pitcher_name: str = field(repr=False, default="")
    pitch_app_id: str = ""
    pitcher_id: int = field(repr=False, default=0)
    batter_id: int = field(repr=False, default=0)
    pitcher_team_id_bb: str = field(repr=False, default="")
    opponent_team_id_bb: str = field(repr=False, default="")
    bb_game_id: str = field(repr=False, default="")
    bbref_game_id: str = field(repr=False, default="")
    park_sv_id: str = field(repr=False, default="")
    play_guid: str = field(repr=False, default="")
    ab_total: int = field(repr=False, default=0)
    ab_count: int = field(repr=False, default=0)
    ab_id: int = field(repr=False, default=0)
    des: str = field(repr=False, default="")
    type: str = field(repr=False, default="")
    id: int = 0
    sz_top: float = field(repr=False, default=0.0)
    sz_bot: float = field(repr=False, default=0.0)
    mlbam_pitch_name: str = ""
    zone_location: int = field(repr=False, default=0)
    stand: str = field(repr=False, default="")
    strikes: int = field(repr=False, default=0)
    balls: int = field(repr=False, default=0)
    p_throws: str = field(repr=False, default="")
    pdes: str = field(repr=False, default="")
    inning: int = field(repr=False, default=0)
    pfx_x: float = field(repr=False, default=0.0)
    pfx_z: float = field(repr=False, default=0.0)
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
    plate_time: float = field(repr=False, default=0.0)
    extension: float = field(repr=False, default=0.0)
    break_angle: float = field(repr=False, default=0.0)
    break_length: float = field(repr=False, default=0.0)
    break_y: float = field(repr=False, default=0.0)
    spin_rate: int = field(repr=False, default=0)
    spin_direction: int = field(repr=False, default=0)
    launch_speed: float = field(repr=False, default=0.0)
    launch_angle: float = field(repr=False, default=0.0)
    total_distance: float = field(repr=False, default=0.0)
    trajectory: str = field(repr=False, default="")
    hardness: str = field(repr=False, default="")
    location: int = field(repr=False, default=0)
    coord_x: float = field(repr=False, default=0.0)
    coord_y: float = field(repr=False, default=0.0)
    game_start_time_str: str = field(repr=False, default="")
    time_pitch_thrown_str: str = field(repr=False, default="")
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
    is_in_play: bool = field(repr=False, default=False)
    is_ground_ball: bool = field(repr=False, default=False)
    is_fly_ball: bool = field(repr=False, default=False)
    is_line_drive: bool = field(repr=False, default=False)
    is_popup: bool = field(repr=False, default=False)
    is_hard_hit: bool = field(repr=False, default=False)
    is_medium_hit: bool = field(repr=False, default=False)
    is_soft_hit: bool = field(repr=False, default=False)
    is_barreled: bool = field(repr=False, default=False)
    is_final_pitch_of_ab: bool = field(repr=False, default=False)
    ab_result_out: bool = field(repr=False, default=False)
    ab_result_hit: bool = field(repr=False, default=False)
    ab_result_single: bool = field(repr=False, default=False)
    ab_result_double: bool = field(repr=False, default=False)
    ab_result_triple: bool = field(repr=False, default=False)
    ab_result_homerun: bool = field(repr=False, default=False)
    ab_result_bb: bool = field(repr=False, default=False)
    ab_result_ibb: bool = field(repr=False, default=False)
    ab_result_k: bool = field(repr=False, default=False)
    ab_result_hbp: bool = field(repr=False, default=False)
    ab_result_error: bool = field(repr=False, default=False)
    ab_result_sac_hit: bool = field(repr=False, default=False)
    ab_result_sac_fly: bool = field(repr=False, default=False)
    ab_result_unclear: bool = field(repr=False, default=False)
    pbp_play_result: str = field(repr=False, default="")
    pbp_runs_outs_result: str = field(repr=False, default="")
    is_patched: bool = field(repr=False, default=False)
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
        thrown_replace_tz = timestamp.replace(tzinfo=TIME_ZONE_NEW_YORK)
        thrown_as_tz = timestamp.replace(tzinfo=timezone.utc).astimezone(TIME_ZONE_NEW_YORK)
        if not self.game_start_time:
            return thrown_as_tz
        seconds_since_thrown_replace_tz = (thrown_replace_tz - self.game_start_time).total_seconds()
        seconds_since_thrown_as_tz = (thrown_as_tz - self.game_start_time).total_seconds()
        if seconds_since_thrown_as_tz < 0 and seconds_since_thrown_replace_tz < 0:
            return None
        if seconds_since_thrown_as_tz > 0 and seconds_since_thrown_replace_tz < 0:
            return thrown_as_tz
        if seconds_since_thrown_as_tz < 0 and seconds_since_thrown_replace_tz > 0:
            return thrown_replace_tz
        return thrown_as_tz if seconds_since_thrown_as_tz < seconds_since_thrown_replace_tz else thrown_replace_tz

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
            "mlbam_pitch_name": self.mlbam_pitch_name,
            "zone_location": self.zone_location,
            "stand": self.stand,
            "strikes": self.strikes,
            "balls": self.balls,
            "p_throws": self.p_throws,
            "pdes": self.pdes,
            "inning": self.inning,
            "pfx_x": self.pfx_x,
            "pfx_z": self.pfx_z,
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
            "plate_time": self.plate_time,
            "extension": self.extension,
            "break_angle": self.break_angle,
            "break_length": self.break_length,
            "break_y": self.break_y,
            "spin_rate": self.spin_rate,
            "spin_direction": self.spin_direction,
            "launch_speed": self.launch_speed,
            "launch_angle": self.launch_angle,
            "total_distance": self.total_distance,
            "trajectory": self.trajectory,
            "hardness": self.hardness,
            "location": self.location,
            "coord_x": self.coord_x,
            "coord_y": self.coord_y,
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
            "is_in_play": self.is_in_play,
            "is_ground_ball": self.is_ground_ball,
            "is_fly_ball": self.is_fly_ball,
            "is_line_drive": self.is_line_drive,
            "is_popup": self.is_popup,
            "is_hard_hit": self.is_hard_hit,
            "is_medium_hit": self.is_medium_hit,
            "is_soft_hit": self.is_soft_hit,
            "is_barreled": self.is_barreled,
            "is_final_pitch_of_ab": self.is_final_pitch_of_ab,
            "ab_result_out": self.is_final_pitch_of_ab,
            "ab_result_hit": self.is_final_pitch_of_ab,
            "ab_result_single": self.is_final_pitch_of_ab,
            "ab_result_double": self.is_final_pitch_of_ab,
            "ab_result_triple": self.is_final_pitch_of_ab,
            "ab_result_homerun": self.is_final_pitch_of_ab,
            "ab_result_bb": self.is_final_pitch_of_ab,
            "ab_result_ibb": self.is_final_pitch_of_ab,
            "ab_result_k": self.is_final_pitch_of_ab,
            "ab_result_hbp": self.is_final_pitch_of_ab,
            "ab_result_error": self.ab_result_error,
            "ab_result_sac_hit": self.is_final_pitch_of_ab,
            "ab_result_sac_fly": self.is_final_pitch_of_ab,
            "ab_result_unclear": self.ab_result_unclear,
            "pbp_play_result": self.pbp_play_result,
            "pbp_runs_outs_result": self.pbp_runs_outs_result,
            "pitch_type_int": int(PitchType.from_abbrev(self.mlbam_pitch_name)),
            "is_patched": self.is_patched,
            "is_invalid_ibb": self.is_invalid_ibb,
            "is_out_of_sequence": self.is_out_of_sequence,
        }

    def get_time_pitch_thrown_str(self):
        return self.time_pitch_thrown.strftime(DT_AWARE) if self.time_pitch_thrown else ""
