class BBRefPlayByPlayEvent():
    """Various numeric and string values that describe a plate appearance."""

    inning_id = None

    def __init__(
        self,
        inning_label=None,
        pbp_table_row_number=None,
        score=None,
        outs_before_play=None,
        runners_on_base=None,
        pitch_sequence=None,
        runs_outs_result=None,
        team_batting_id_br=None,
        team_pitching_id_br=None,
        play_description=None,
        pitcher_id_br=None,
        batter_id_br=None
    ):
        self.inning_label = inning_label
        self.pbp_table_row_number = int(pbp_table_row_number) if pbp_table_row_number else None
        self.score = score
        self.outs_before_play = outs_before_play
        self.runners_on_base = runners_on_base
        self.pitch_sequence = pitch_sequence
        self.runs_outs_result = runs_outs_result
        self.team_batting_id_br = team_batting_id_br
        self.team_pitching_id_br = team_pitching_id_br
        self.play_description = play_description
        self.pitcher_id_br = pitcher_id_br
        self.batter_id_br = batter_id_br

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