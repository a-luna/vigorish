"""Pitch logs and pitchfx URLs for a single game."""
import json
from dataclasses import dataclass, field

from vigorish.scrape.brooks_pitch_logs.models.pitch_log import BrooksPitchLog
from vigorish.util.list_helpers import as_dict_list
from vigorish.util.string_helpers import validate_bbref_game_id


@dataclass
class BrooksPitchLogsForGame:
    """Pitch logs and pitchfx URLs for a single game."""

    bb_game_id: str = ""
    bbref_game_id: str = ""
    pitch_log_count: str = ""
    pitch_logs: list[BrooksPitchLog] = field(default_factory=list)

    @property
    def game_date(self):
        result = validate_bbref_game_id(self.bbref_game_id)
        return result.value["game_date"] if result.success else None

    @property
    def all_pitch_app_ids(self):
        return [pitch_log.pitch_app_id for pitch_log in self.pitch_logs]

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
