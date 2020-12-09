class BBRefUmpire:
    """Name and field location of one umpire for a single game."""

    field_location = ""
    umpire_name = ""

    def as_dict(self):
        """Convert umpire name and field location to a dictionary."""
        dict = {
            "field_location": f"{self.field_location}",
            "umpire_name": f"{self.umpire_name}",
        }
        return dict
