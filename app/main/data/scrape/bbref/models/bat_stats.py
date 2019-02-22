class BBrefBatStats():
    """Statistics for all plate appearances that occurred for one player in a single game."""

    player_bbref_id = ""
    at_bats = ""
    runs_scored = ""
    hits = ""
    rbis = ""
    bases_on_balls = ""
    strikeouts = ""
    plate_appearances = ""
    avg_to_date = ""
    obp_to_date = ""
    slg_to_date = ""
    ops_to_date = ""
    total_pitches = ""
    total_strikes = ""
    wpa_bat = ""
    avg_lvg_index = ""
    wpa_bat_pos = ""
    wpa_bat_neg = ""
    re24_bat = ""
    details = []

    def as_dict(self):
        """Convert batting statistics for one player in a single game to a dictionary."""
        dict = {
            "player_bbref_id": "{}".format(self.player_bbref_id),
            "at_bats": int(self.at_bats),
            "runs_scored": int(self.runs_scored),
            "hits": int(self.hits),
            "rbis": int(self.rbis),
            "bases_on_balls": int(self.bases_on_balls),
            "strikeouts": int(self.strikeouts),
            "plate_appearances": int(self.plate_appearances),
            "avg_to_date": float(self.avg_to_date),
            "obp_to_date": float(self.obp_to_date),
            "slg_to_date": float(self.slg_to_date),
            "ops_to_date": float(self.ops_to_date),
            "total_pitches": int(self.total_pitches),
            "total_strikes": int(self.total_strikes),
            "wpa_bat": float(self.wpa_bat),
            "avg_lvg_index": float(self.avg_lvg_index),
            "wpa_bat_pos": float(self.wpa_bat_pos),
            "wpa_bat_neg": float(self.wpa_bat_neg),
            "re24_bat": float(self.re24_bat),
            "details": self._flatten(self.details)
        }
        return dict


    @staticmethod
    def _flatten(objects):
        return [obj.as_dict() for obj in objects]