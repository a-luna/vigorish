"""Weather conditions at game start time and other descriptive info."""


class BBRefBoxscoreMeta:
    """Weather conditions at game start time and other descriptive info."""

    park_name = ""
    field_type = ""
    day_night = ""
    first_pitch_temperature = ""
    first_pitch_precipitation = ""
    first_pitch_wind = ""
    first_pitch_clouds = ""
    game_duration = ""
    attendance = ""

    def as_dict(self):
        """Convert boxscore meta info to a dictionary."""
        dict = {
            "__bbref_boxscore_meta__": True,
            "park_name": "{}".format(self.park_name),
            "field_type": "{}".format(self.field_type),
            "day_night": "{}".format(self.day_night),
            "first_pitch_temperature": int(self.first_pitch_temperature),
            "first_pitch_precipitation": "{}".format(self.first_pitch_precipitation),
            "first_pitch_wind": "{}".format(self.first_pitch_wind),
            "first_pitch_clouds": "{}".format(self.first_pitch_clouds),
            "game_duration": "{}".format(self.game_duration),
            "attendance": int(self.attendance),
        }
        return dict
