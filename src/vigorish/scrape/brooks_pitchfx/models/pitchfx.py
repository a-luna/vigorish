"""Pitchfx measurements for a single pitch scraped from brooksbaseball.com."""
import re
from dataclasses import dataclass
from datetime import datetime, timezone

from vigorish.util.datetime_util import TIME_ZONE_NEW_YORK
from vigorish.util.string_helpers import validate_bbref_game_id

TIMESTAMP_REGEX = re.compile(
    r"(?P<year>\d{2,2})"
    r"(?P<month>\d{2,2})"
    r"(?P<day>\d{2,2})_"
    r"(?P<hour>\d{2,2})"
    r"(?P<minute>\d{2,2})"
    r"(?P<second>\d{2,2})"
    r"(?P<team_id>[a-z]{3,3})"
)


@dataclass
class BrooksPitchFxData:
    """Pitchfx measurements for a single pitch scraped from brooksbaseball.com."""

    pitcher_name: str = ""
    pitch_app_id: str = ""
    pitcher_id: str = "0"
    batter_id: str = "0"
    pitcher_team_id_bb: str = ""
    opponent_team_id_bb: str = ""
    bb_game_id: str = ""
    bbref_game_id: str = ""
    table_row_number: str = "0"
    park_sv_id: str = ""
    play_guid: str = ""
    ab_total: str = "0"
    ab_count: str = "0"
    ab_id: str = "0"
    des: str = ""
    type: str = ""
    id: str = "0"
    sz_top: str = "0"
    sz_bot: str = "0"
    pfx_xdatafile: str = "0"
    pfx_zdatafile: str = "0"
    mlbam_pitch_name: str = ""
    zone_location: str = "0"
    pitch_con: str = "0"
    stand: str = ""
    strikes: str = "0"
    balls: str = "0"
    p_throws: str = ""
    pdes: str = ""
    spin: str = "0"
    norm_ht: str = "0"
    inning: str = "0"
    tstart: str = "0"
    vystart: str = "0"
    ftime: str = "0"
    pfx_x: str = "0"
    pfx_z: str = "0"
    uncorrected_pfx_x: str = "0"
    uncorrected_pfx_z: str = "0"
    x0: str = "0"
    y0: str = "0"
    z0: str = "0"
    vx0: str = "0"
    vy0: str = "0"
    vz0: str = "0"
    ax: str = "0"
    ay: str = "0"
    az: str = "0"
    start_speed: str = "0"
    px: str = "0"
    pz: str = "0"
    pxold: str = "0"
    pzold: str = "0"
    tm_spin: str = "0"
    sb: str = "0"

    @property
    def timestamp_pitch_thrown(self):
        match = TIMESTAMP_REGEX.match(self.park_sv_id)
        if not match:
            return None
        group_dict = match.groupdict()
        result = validate_bbref_game_id(self.bbref_game_id)
        if result.failure:
            return None
        game_date = result.value["game_date"]
        timestamp = datetime(
            game_date.year,
            int(group_dict["month"]),
            int(group_dict["day"]),
            int(group_dict["hour"]),
            int(group_dict["minute"]),
            int(group_dict["second"]),
        )
        return timestamp.replace(tzinfo=timezone.utc).astimezone(TIME_ZONE_NEW_YORK)

    @property
    def has_zone_location(self):
        return False if self.zone_location == 99 else True

    def as_dict(self):
        """Convert pitch log to a dictionary."""
        return dict(
            __brooks_pitchfx_data__=True,
            pitcher_name=self.pitcher_name,
            pitch_app_id=self.pitch_app_id,
            pitcher_id=self.pitcher_id,
            batter_id=self.batter_id,
            pitcher_team_id_bb=self.pitcher_team_id_bb,
            opponent_team_id_bb=self.opponent_team_id_bb,
            bb_game_id=self.bb_game_id,
            bbref_game_id=self.bbref_game_id,
            table_row_number=int(self.table_row_number),
            park_sv_id=self.park_sv_id,
            play_guid=self.play_guid,
            ab_total=int(self.ab_total),
            ab_count=int(self.ab_count),
            ab_id=int(self.ab_id),
            des=self.des,
            type=self.type,
            id=int(self.id),
            sz_top=float(self.sz_top),
            sz_bot=float(self.sz_bot),
            pfx_xdatafile=float(self.pfx_xdatafile),
            pfx_zdatafile=float(self.pfx_zdatafile),
            mlbam_pitch_name=self.mlbam_pitch_name,
            zone_location=self.zone_location,
            pitch_con=float(self.pitch_con),
            stand=self.stand,
            strikes=int(self.strikes),
            balls=int(self.balls),
            p_throws=self.p_throws,
            pdes=self.pdes,
            spin=float(self.spin),
            norm_ht=float(self.norm_ht),
            inning=int(self.inning),
            tstart=float(self.tstart),
            vystart=float(self.vystart),
            ftime=float(self.ftime),
            pfx_x=float(self.pfx_x),
            pfx_z=float(self.pfx_z),
            uncorrected_pfx_x=float(self.uncorrected_pfx_x),
            uncorrected_pfx_z=float(self.uncorrected_pfx_z),
            x0=float(self.x0),
            y0=float(self.y0),
            z0=float(self.z0),
            vx0=float(self.vx0),
            vy0=float(self.vy0),
            vz0=float(self.vz0),
            ax=float(self.ax),
            ay=float(self.ay),
            az=float(self.az),
            start_speed=float(self.start_speed),
            px=float(self.px),
            pz=float(self.pz),
            pxold=float(self.pxold),
            pzold=float(self.pzold),
            tm_spin=int(self.tm_spin),
            sb=int(self.sb),
        )