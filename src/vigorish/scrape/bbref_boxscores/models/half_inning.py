from vigorish.util.list_helpers import as_dict_list


class BBRefHalfInning:
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
    misc_events = []

    def as_dict(self):
        """Convert half-inning to a dictionary."""
        return dict(
            __bbref_boxscore_half_inning__=True,
            inning_id=self.inning_id,
            inning_label=self.inning_label,
            begin_inning_summary=self.begin_inning_summary,
            end_inning_summary=self.end_inning_summary,
            inning_total_runs=int(self.inning_total_runs),
            inning_total_hits=int(self.inning_total_hits),
            inning_total_errors=int(self.inning_total_errors),
            inning_total_left_on_base=int(self.inning_total_left_on_base),
            away_team_runs_after_inning=int(self.away_team_runs_after_inning),
            home_team_runs_after_inning=int(self.home_team_runs_after_inning),
            game_events=as_dict_list(self.game_events),
            substitutions=as_dict_list(self.substitutions),
            misc_events=as_dict_list(self.misc_events),
        )
