from dataclasses import dataclass


@dataclass
class BBRefGameInfo:
    """Individual game info scraped from brooksbaseball.com."""

    url: str = ""
    bbref_game_id: str = ""

    def as_dict(self):
        """Convert game info to a dictionary."""
        return {
            "__bbref_game_info__": True,
            "url": self.url,
            "bbref_game_id": self.bbref_game_id,
        }
