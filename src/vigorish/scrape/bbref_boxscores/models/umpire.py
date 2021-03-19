from dataclasses import dataclass


@dataclass
class BBRefUmpire:
    """Name and field location of one umpire for a single game."""

    field_location: str = ""
    umpire_name: str = ""

    def as_dict(self):
        """Convert umpire name and field location to a dictionary."""
        return {
            "field_location": f"{self.field_location}",
            "umpire_name": f"{self.umpire_name}",
        }
