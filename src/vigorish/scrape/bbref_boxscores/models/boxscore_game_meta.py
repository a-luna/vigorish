"""Weather conditions at game start time and other descriptive info."""
from dataclasses import dataclass


@dataclass
class BBRefBoxscoreMeta:
    """Weather conditions at game start time and other descriptive info."""

    park_name: str = ""
    field_type: str = ""
    day_night: str = ""
    first_pitch_temperature: int = 0
    first_pitch_precipitation: str = ""
    first_pitch_wind: str = ""
    first_pitch_clouds: str = ""
    game_duration: str = ""
    attendance: int = 0

    def as_dict(self):
        """Convert boxscore meta info to a dictionary."""
        return {
            "__bbref_boxscore_meta__": True,
            "park_name": self.park_name,
            "field_type": self.field_type,
            "day_night": self.day_night,
            "first_pitch_temperature": self.first_pitch_temperature,
            "first_pitch_precipitation": self.first_pitch_precipitation,
            "first_pitch_wind": self.first_pitch_wind,
            "first_pitch_clouds": self.first_pitch_clouds,
            "game_duration": self.game_duration,
            "attendance": self.attendance,
        }
