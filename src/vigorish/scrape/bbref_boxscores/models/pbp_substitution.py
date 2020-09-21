from dataclasses import dataclass

from vigorish.enums import PlayByPlayEvent
from vigorish.util.list_helpers import display_dict


@dataclass
class BBRefInGameSubstitution:
    """Information describing an in-game player substitution."""

    inning_id: str = ""
    inning_label: str = ""
    pbp_table_row_number: str = "0"
    sub_description: str = ""
    incoming_player_name: str = ""
    outgoing_player_name: str = ""
    incoming_player_id_br: str = ""
    outgoing_player_id_br: str = ""
    incoming_player_pos: str = ""
    outgoing_player_pos: str = ""
    lineup_slot: str = "0"
    sub_type: str = ""
    team_id: str = ""

    @property
    def event_type(self):
        return PlayByPlayEvent.SUBSTITUTION

    def as_dict(self):
        dict = {
            "__bbref_pbp_in_game_substitution__": True,
            "inning_id": self.inning_id,
            "inning_label": self.inning_label,
            "pbp_table_row_number": int(self.pbp_table_row_number),
            "sub_type": self.sub_type,
            "team_id": self.team_id,
            "sub_description": self.sub_description,
            "incoming_player_id_br": self.incoming_player_id_br,
            "incoming_player_pos": self.incoming_player_pos,
            "outgoing_player_id_br": self.outgoing_player_id_br,
            "outgoing_player_pos": self.outgoing_player_pos,
            "lineup_slot": int(self.lineup_slot),
        }
        return dict

    def display(self):
        display_dict(self.as_dict())
