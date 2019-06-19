"""Pitch logs and pitchfx URLs for a single game."""
import json

from app.main.util.string_functions import validate_bb_game_id
from app.main.util.list_functions import as_dict_list

class BrooksPitchLogsForGame():
    """Pitch logs and pitchfx URLs for a single game."""

    bb_game_id = ""
    bbref_game_id = ""
    pitch_log_count = ""
    pitch_logs = []

    @property
    def game_id_dict(self):
        return {f'{self.bbref_game_id}': f'{self.bb_game_id}'}

    @property
    def game_date(self):
        result = validate_bb_game_id(self.bb_game_id)
        if result.failure:
            return None
        game_dict = result.value
        return game_dict['game_date']

    @property
    def upload_id(self):
        return self.bbref_game_id

    def as_dict(self):
        """Convert pitch logs for game to a dictionary."""
        return dict(
            __brooks_pitch_logs_for_game__=True,
            bb_game_id=self.bb_game_id,
            bbref_game_id=self.bbref_game_id,
            pitch_log_count=int(self.pitch_log_count),
            pitch_logs=as_dict_list(self.pitch_logs))

    def as_json(self):
        """Convert pitch logs for game to JSON."""
        return json.dumps(self.as_dict(), indent=2)
