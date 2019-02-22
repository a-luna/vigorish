class BBrefInningRunsScored():
    """The number of runs scored in one inning by a team."""

    team_id = ""
    inning = ""
    runs = ""

    def as_dict(self):
        """Convert a single inning run total for a team to a dictionary."""
        dict = {
            "team_id": "{}".format(self.team_id),
            "inning": int(self.inning),
            "runs": "{}".format(self.runs)
        }
        return dict
