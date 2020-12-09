from dataclasses import dataclass


@dataclass
class BBRefBatStatsDetail:
    """Additional details for important batting events."""

    count: str = ""
    stat: str = ""

    def as_dict(self):
        """Convert detailed batting stats to a dictionary."""
        return {"count": int(self.count), "stat": self.stat}
