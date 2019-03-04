"""A single pitching appearance scraped from brooksbaseball.com."""

class BrooksPitchLog():
    """A single pitching appearance scraped from brooksbaseball.com."""

    pitcher_name = ""
    pitcher_id_mlb = ""
    pitch_app_id = ""
    total_pitch_count = ""
    pitch_count_by_inning = {}
    pitcher_team_id_bb = ""
    opponent_team_id_bb = ""
    bb_game_id = ""
    bbref_game_id = ""
    pitchfx_url = ""
    pitch_log_url = ""

    def as_dict(self):
        """Convert pitch log to a dictionary."""
        dict = {
            "__brooks_pitch_log__": True,
            "pitcher_name": "{}".format(self.pitcher_name),
            "pitcher_id_mlb": int(self.pitcher_id_mlb),
            "pitch_app_id": "{}".format(self.pitch_app_id),
            "total_pitch_count": int(self.total_pitch_count),
            "pitch_count_by_inning": self.pitch_count_by_inning,
            "pitcher_team_id_bb": "{}".format(self.pitcher_team_id_bb),
            "opponent_team_id_bb": "{}".format(self.opponent_team_id_bb),
            "bb_game_id": "{}".format(self.bb_game_id),
            "bbref_game_id": "{}".format(self.bbref_game_id),
            "pitchfx_url": "{}".format(self.pitchfx_url),
            "pitch_log_url": "{}".format(self.pitch_log_url)
        }
        return dict
