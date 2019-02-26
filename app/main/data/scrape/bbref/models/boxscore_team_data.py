class BBRefBoxscoreTeamData():
    team_id_br = ''
    total_wins_before_game = None
    total_losses_before_game = None
    total_runs_scored_by_team = None
    total_runs_scored_by_opponent = None
    total_hits_by_team = None
    total_hits_by_opponent = None
    total_errors_by_team = None
    total_errors_by_opponent = None
    starting_lineup = []
    batting_stats = []
    pitching_stats = []

    def as_dict(self):
        return dict(
            __bbref_boxscore_team_data__=True,
            team_id_br = "{}".format(self.team_id_br),
            total_wins_before_game= int(self.total_wins_before_game),
            total_losses_before_game= int(self.total_losses_before_game),
            total_runs_scored_by_team= int(self.total_runs_scored_by_team),
            total_runs_scored_by_opponent= int(self.total_runs_scored_by_opponent),
            total_hits_by_team= int(self.total_hits_by_team),
            total_hits_by_opponent= int(self.total_hits_by_opponent),
            total_errors_by_team= int(self.total_errors_by_team),
            total_errors_by_opponent= int(self.total_errors_by_opponent),
            starting_lineup= self._flatten(self.starting_lineup),
            batting_stats= self._flatten(self.batting_stats),
            pitching_stats= self._flatten(self.pitching_stats)
        )

    @staticmethod
    def _flatten(objects):
        return [obj.as_dict() for obj in objects]