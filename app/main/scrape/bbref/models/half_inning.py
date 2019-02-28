class BBRefHalfInning():
    inning_id = ""
    inning_label = ""
    begin_inning_summary = ""
    end_inning_summary = ""
    inning_total_runs = None
    inning_total_hits = None
    inning_total_errors = None
    inning_total_left_on_base = None
    away_team_runs_after_inning = None
    home_team_runs_after_inning = None
    game_events = []
    substitutions = []

    def as_dict(self):
        """Convert half-inning to a dictionary."""
        dict = {
            "__bbref_boxscore_half_inning__": True,
            "inning_id": "{}".format(self.inning_id),
            "inning_label": "{}".format(self.inning_label),
            "begin_inning_summary": "{}".format(self.begin_inning_summary),
            "end_inning_summary": "{}".format(self.end_inning_summary),
            "inning_total_runs": int(self.inning_total_runs),
            "inning_total_hits": int(self.inning_total_hits),
            "inning_total_errors": int(self.inning_total_errors),
            "inning_total_left_on_base": int(self.inning_total_left_on_base),
            "away_team_runs_after_inning": int(self.away_team_runs_after_inning),
            "home_team_runs_after_inning": int(self.home_team_runs_after_inning),
            "game_events": self._flatten(self.game_events),
            "substitutions": self._flatten(self.substitutions)
        }
        return dict

    @staticmethod
    def _flatten(objects):
        return [obj.as_dict() for obj in objects]
