"""Enum definitions."""
from enum import Enum, auto


class DataSet(Enum):
    """MLB data sets."""

    brooks_games_for_date = auto()
    brooks_pitch_logs = auto()
    brooks_pitchfx = auto()
    bbref_games_for_date = auto()
    bbref_boxscore = auto()

    def __str__(self):
        return self.name
