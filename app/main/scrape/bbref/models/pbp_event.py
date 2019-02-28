class BBRefPlayByPlayEvent():
    """Various numeric and string values that describe a plate appearance."""

    inning_id = ""
    inning_label = ""
    pbp_table_row_number = None
    score = ""
    outs_before_play = ""
    runners_on_base = ""
    pitch_sequence = ""
    runs_outs_result = ""
    team_batting_id_br = ""
    team_pitching_id_br = ""
    play_description = ""
    pitcher_id_br = ""
    batter_id_br = ""

    def as_dict(self):
        """Convert play event values to a dictionary."""
        dict = {
            "__bbref_pbp_game_event__": True,
            "inning_id": "{}".format(self.inning_id),
            "inning_label": "{}".format(self.inning_label),
            'pbp_table_row_number': int(self.pbp_table_row_number),
            "score": "{}".format(self.score),
            "outs_before_play": int(self.outs_before_play),
            "runners_on_base": "{}".format(self.runners_on_base),
            "pitch_sequence": "{}".format(self.pitch_sequence),
            "runs_outs_result": "{}".format(self.runs_outs_result),
            "team_batting_id_br": "{}".format(self.team_batting_id_br),
            "team_pitching_id_br": "{}".format(self.team_pitching_id_br),
            "play_description": "{}".format(self.play_description),
            "pitcher_id_br": "{}".format(self.pitcher_id_br),
            "batter_id_br": "{}".format(self.batter_id_br)
        }
        return dict