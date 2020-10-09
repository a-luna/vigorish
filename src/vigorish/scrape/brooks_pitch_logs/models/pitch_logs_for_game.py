"""Pitch logs and pitchfx URLs for a single game."""
import json

from vigorish.util.list_helpers import as_dict_list
from vigorish.util.string_helpers import validate_brooks_game_id


class BrooksPitchLogsForGame:
    """Pitch logs and pitchfx URLs for a single game."""

    bb_game_id = ""
    bbref_game_id = ""
    pitch_log_count = ""
    pitch_logs = []

    @property
    def game_id_dict(self):
        return {f"{self.bbref_game_id}": f"{self.bb_game_id}"}

    @property
    def game_date(self):
        result = validate_brooks_game_id(self.bb_game_id)
        if result.failure:
            return None
        game_dict = result.value
        return game_dict["game_date"]

    @property
    def total_pitch_count(self):
        return sum(pitch_log.total_pitch_count for pitch_log in self.pitch_logs)

    @property
    def upload_id(self):
        return self.bbref_game_id

    @property
    def pitch_app_ids(self):
        return [pitch_log.pitch_app_id for pitch_log in self.pitch_logs]

    @property
    def pitch_app_ids_scraped_all_data(self):
        return [
            pitch_log.pitch_app_id for pitch_log in self.pitch_logs if pitch_log.parsed_all_info
        ]

    @property
    def pitch_app_ids_no_data(self):
        return [
            pitch_log.pitch_app_id for pitch_log in self.pitch_logs if not pitch_log.parsed_all_info
        ]

    def as_dict(self):
        """Convert pitch logs for game to a dictionary."""
        return dict(
            __brooks_pitch_logs_for_game__=True,
            bb_game_id=self.bb_game_id,
            bbref_game_id=self.bbref_game_id,
            pitch_log_count=int(self.pitch_log_count),
            pitch_logs=as_dict_list(self.pitch_logs),
        )

    def as_json(self):
        """Convert pitch logs for game to JSON."""
        return json.dumps(self.as_dict(), indent=2)
