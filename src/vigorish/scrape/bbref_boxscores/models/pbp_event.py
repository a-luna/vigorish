from dataclasses import dataclass

from vigorish.constants import PPB_PITCH_LOG_DICT
from vigorish.enums import PlayByPlayEvent


@dataclass
class BBRefPlayByPlayEvent:
    """Various numeric and string values that describe a plate appearance."""

    event_id: str = "0"
    inning_id: str = "0"
    inning_label: str = "0"
    pbp_table_row_number: str = "0"
    score: str = "0"
    outs_before_play: str = "0"
    runners_on_base: str = "0"
    pitch_sequence: str = "0"
    runs_outs_result: str = "0"
    team_batting_id_br: str = "0"
    team_pitching_id_br: str = "0"
    play_description: str = "0"
    pitcher_id_br: str = "0"
    batter_id_br: str = "0"
    play_index_url: str = "0"

    @property
    def event_type(self):
        return PlayByPlayEvent.AT_BAT

    def pitch_sequence_description(self):
        total_pitches_in_sequence = self.pitch_count()
        current_pitch_count = 0
        sequence_description = []
        for abbrev in self.pitch_sequence:
            pitch_description = PPB_PITCH_LOG_DICT[abbrev]["description"]
            if PPB_PITCH_LOG_DICT[abbrev]["pitch_counts"]:
                current_pitch_count += 1
                pitch_number = f"Pitch {current_pitch_count}/{total_pitches_in_sequence}"
                sequence_description.append(f"{pitch_number}: {pitch_description}")
            else:
                sequence_description.append(pitch_description)
        return sequence_description

    def pitch_count(self):
        return sum(PPB_PITCH_LOG_DICT[abbrev]["pitch_counts"] for abbrev in self.pitch_sequence)

    def as_dict(self):
        """Convert game event values to a dictionary."""
        dict = {
            "__bbref_pbp_game_event__": True,
            "event_id": self.event_id,
            "inning_id": self.inning_id,
            "inning_label": self.inning_label,
            "pbp_table_row_number": int(self.pbp_table_row_number),
            "score": self.score,
            "outs_before_play": int(self.outs_before_play),
            "runners_on_base": self.runners_on_base,
            "pitch_sequence": self.pitch_sequence,
            "runs_outs_result": self.runs_outs_result,
            "team_batting_id_br": self.team_batting_id_br,
            "team_pitching_id_br": self.team_pitching_id_br,
            "play_description": self.play_description,
            "pitcher_id_br": self.pitcher_id_br,
            "batter_id_br": self.batter_id_br,
            "play_index_url": self.play_index_url,
        }
        return dict
