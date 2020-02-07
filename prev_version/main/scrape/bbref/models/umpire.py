class BBRefUmpire:
    """Name and field location of one umpire for a single game."""

    field_location = ""
    umpire_name = ""

    def as_dict(self):
        """Convert umpire name and field location to a dictionary."""
        dict = {
            "field_location": "{loc}".format(loc=self.field_location),
            "umpire_name": "{n}".format(n=self.umpire_name),
        }
        return dict
