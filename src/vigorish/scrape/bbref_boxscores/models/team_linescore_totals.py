class BBRefTeamLinescoreTotals:
    """Total runs, hits and errors credited to a team in one game."""

    team_id_br = ""
    total_runs = ""
    total_hits = ""
    total_errors = ""

    def as_dict(self):
        """Convert total runs, hits and errors for one team in a single game to a dictionary."""
        dict = {
            "team_id_br": "{}".format(self.team_id_br),
            "total_runs": int(self.total_runs),
            "total_hits": int(self.total_hits),
            "total_errors": int(self.total_errors),
        }
        return dict
