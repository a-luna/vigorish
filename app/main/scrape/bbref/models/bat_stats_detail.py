class BBRefBatStatsDetail():
    """Additional details for important batting events."""

    count = ""
    stat = ""

    def as_dict(self):
        """Convert detailed batting stats to a dictionary."""
        dict = {
            "count": int(self.count),
            "stat": "{}".format(self.stat)
        }
        return dict