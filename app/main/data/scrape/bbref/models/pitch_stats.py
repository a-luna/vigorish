class BBRefPitchStats():
    """Statistics for a an individual pitching appearance in a single game."""

    player_id_br = ""
    player_team_id_br = ""
    opponent_team_id_br = ""
    innings_pitched = ""
    hits = ""
    runs = ""
    earned_runs = ""
    bases_on_balls = ""
    strikeouts = ""
    homeruns = ""
    batters_faced = ""
    pitch_count = ""
    strikes = ""
    strikes_contact = ""
    strikes_swinging = ""
    strikes_looking = ""
    ground_balls = ""
    fly_balls = ""
    line_drives = ""
    unknown_type = ""
    game_score = ""
    inherited_runners = ""
    inherited_scored = ""
    wpa_pitch = ""
    avg_lvg_index = ""
    re24_pitch = ""

    def as_dict(self):
        """Convert a single player's pitching appearance to a dictionary."""
        dict = {
                "player_id_br": "{}".format(self.player_id_br),
                "player_team_id_br": "{}".format(self.player_team_id_br),
                "opponent_team_id_br": "{}".format(self.opponent_team_id_br),
                "innings_pitched": float(self.innings_pitched),
                "hits": int(self.hits),
                "runs": int(self.runs),
                "earned_runs": int(self.earned_runs),
                "bases_on_balls": int(self.bases_on_balls),
                "strikeouts": int(self.strikeouts),
                "homeruns": int(self.homeruns),
                "batters_faced": int(self.batters_faced),
                "pitch_count": int(self.pitch_count),
                "strikes": int(self.strikes),
                "strikes_contact": int(self.strikes_contact),
                "strikes_swinging": int(self.strikes_swinging),
                "strikes_looking": int(self.strikes_looking),
                "ground_balls": int(self.ground_balls),
                "fly_balls": int(self.fly_balls),
                "line_drives": int(self.line_drives),
                "unknown_type": int(self.unknown_type),
                "game_score": int(self.game_score),
                "inherited_runners": int(self.inherited_runners),
                "inherited_scored": int(self.inherited_scored),
                "wpa_pitch": float(self.wpa_pitch),
                "avg_lvg_index": float(self.avg_lvg_index),
                "re24_pitch": float(self.re24_pitch)
        }
        return dict