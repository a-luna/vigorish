"""Pitch logs and pitchfx URLs for a single game."""
import json
from dataclasses import dataclass, field
from typing import List

from vigorish.scrape.brooks_pitch_logs.models.pitch_log import BrooksPitchLog
from vigorish.util.list_helpers import as_dict_list


@dataclass
class BrooksPitchLogsForGame:
    """Pitch logs and pitchfx URLs for a single game."""

    bb_game_id: str = ""
    bbref_game_id: str = ""
    pitch_log_count: str = ""
    pitch_logs: List[BrooksPitchLog] = field(default_factory=list)

    def as_dict(self):
        """Convert pitch logs for game to a dictionary."""
        return {
            "__brooks_pitch_logs_for_game__": True,
            "bb_game_id": self.bb_game_id,
            "bbref_game_id": self.bbref_game_id,
            "pitch_log_count": int(self.pitch_log_count),
            "pitch_logs": as_dict_list(self.pitch_logs),
        }

    def as_json(self):
        """Convert pitch logs for game to JSON."""
        return json.dumps(self.as_dict(), indent=2)
