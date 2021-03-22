from dataclasses import dataclass


@dataclass
class BBRefStartingLineupSlot:
    """Batting order and defensive position for a single player in a starting lineup."""

    player_id_br: str = ""
    bat_order: str = ""
    def_position: str = ""

    def as_dict(self):
        """Convert lineup details to a dictionary."""
        return {
            "player_id_br": f"{self.player_id_br}",
            "bat_order": int(self.bat_order),
            "def_position": f"{self.def_position}",
        }
