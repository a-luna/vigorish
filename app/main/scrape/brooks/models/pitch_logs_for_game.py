"""Pitch logs and pitchfx URLs for a single game."""
import json

class BrooksPitchLogsForGame():
    """Pitch logs and pitchfx URLs for a single game."""

    bb_game_id = ""
    bbref_game_id = ""
    pitch_log_count = ""
    pitch_logs = []

    @property
    def upload_id(self):
        return self.bb_game_id

    def as_dict(self):
        """Convert pitch logs for game to a dictionary."""
        dict = {
            "__brooks_pitch_logs_for_game__": True,
            "bb_game_id": "{}".format(self.bb_game_id),
            "bbref_game_id": "{}".format(self.bbref_game_id),
            "pitch_log_count": int(self.pitch_log_count),
            "pitch_logs": self._flatten(self.pitch_logs)
        }
        return dict

    def as_json(self):
        """Convert pitch logs for game to JSON."""
        return json.dumps(self.as_dict(), indent=2)

    @staticmethod
    def _flatten(objects):
        return [obj.as_dict() for obj in objects]
