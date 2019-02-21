"""Pitchfx measurements for a single pitch scraped from brooksbaseball.com."""

class PitchFxItem():
    """Pitchfx measurements for a single pitch scraped from brooksbaseball.com."""

    scrape_success = ""
    scrape_error = ""
    pitchfx_url = ""
    pitch_app_guid = ""
    pitcher_name = ""
    datestamp = ""
    park_sv_id = ""
    play_guid = ""
    ab_total = ""
    ab_count = ""
    pitcher_id_mlb = ""
    batter_id_mlb = ""
    ab_id = ""
    des = ""
    type = ""
    id = ""
    sz_top = ""
    sz_bot = ""
    pfx_xdatafile = ""
    pfx_zdatafile = ""
    mlbam_pitch_name = ""
    zone_location = ""
    pitch_con = ""
    stand = ""
    strikes = ""
    balls = ""
    p_throws = ""
    gid = ""
    pdes = ""
    spin = ""
    norm_ht = ""
    inning = ""
    pitcher_team = ""
    tstart = ""
    vystart = ""
    ftime = ""
    pfx_x = ""
    pfx_z = ""
    uncorrected_pfx_x = ""
    uncorrected_pfx_z = ""
    x0 = ""
    y0 = ""
    z0 = ""
    vx0 = ""
    vy0 = ""
    vz0 = ""
    ax = ""
    ay = ""
    az = ""
    start_speed = ""
    px = ""
    pz = ""
    pxold = ""
    pzold = ""
    tm_spin = ""
    sb = ""

    def as_dict(self):
        """Convert pitch log to a dictionary."""
        dict = {
            "scrape_success": "{}".format(self.scrape_success),
            "scrape_error": "{}".format(self.scrape_error),
            "pitch_app_guid": "{}".format(self.pitch_app_guid),
            "pitcher_name": "{}".format(self.pitcher_name),
            "datestamp": "{}".format(self.datestamp),
            "park_sv_id": "{}".format(self.park_sv_id),
            "play_guid": "{}".format(self.play_guid),
            "ab_total": int(self.ab_total),
            "ab_count": int(self.ab_count),
            "pitcher_id_mlb": "{}".format(self.pitcher_id_mlb),
            "batter_id_mlb": "{}".format(self.batter_id_mlb),
            "ab_id": int(self.ab_id),
            "des": "{}".format(self.des),
            "type": "{}".format(self.type),
            "id": int(self.id),
            "sz_top": float(self.sz_top),
            "sz_bot": float(self.sz_bot),
            "pfx_xdatafile": float(self.pfx_xdatafile),
            "pfx_zdatafile": float(self.pfx_zdatafile),
            "mlbam_pitch_name": "{}".format(self.mlbam_pitch_name),
            "zone_location": "{}".format(self.zone_location),
            "pitch_con": float(self.pitch_con),
            "stand": "{}".format(self.stand),
            "strikes": int(self.strikes),
            "balls": int(self.balls),
            "p_throws": "{}".format(self.p_throws),
            "pdes": "{}".format(self.pdes),
            "spin": float(self.spin),
            "norm_ht": float(self.norm_ht),
            "inning": int(self.inning),
            "pitcher_team": "{}".format(self.pitcher_team),
            "tstart": float(self.tstart),
            "vystart": float(self.vystart),
            "ftime": float(self.ftime),
            "pfx_x": float(self.pfx_x),
            "pfx_z": float(self.pfx_z),
            "uncorrected_pfx_x": float(self.uncorrected_pfx_x),
            "uncorrected_pfx_z": float(self.uncorrected_pfx_z),
            "x0": float(self.x0),
            "y0": float(self.y0),
            "z0": float(self.z0),
            "vx0": float(self.vx0),
            "vy0": float(self.vy0),
            "vz0": float(self.vz0),
            "ax": float(self.ax),
            "ay": float(self.ay),
            "az": float(self.az),
            "start_speed": float(self.start_speed),
            "px": float(self.px),
            "pz": float(self.pz),
            "pxold": float(self.pxold),
            "pzold": float(self.pzold),
            "tm_spin": int(self.tm_spin),
            "sb": int(self.sb),
            "gid": "{}".format(self.gid),
            "pitchfx_url": "{}".format(self.pitchfx_url)
        }
        return dict