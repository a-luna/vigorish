"""Enum definitions."""
from enum import Enum, auto


class DataSet(Enum):
    """MLB data sets."""

    BBREF_GAMES_FOR_DATE = auto()
    BBREF_BOXSCORES = auto()
    BROOKS_GAMES_FOR_DATE = auto()
    BROOKS_PITCH_LOGS = auto()
    BROOKS_PITCHFX = auto()
