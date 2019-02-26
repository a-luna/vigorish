import re

from app.main.util.list_functions import display_dict

class BBRefInGameSubstitution():
    inning = None
    pbp_table_row_number = None
    sub_description = None
    incoming_player_id_br = None
    outgoing_player_id_br = None
    incoming_player_pos = None
    outgoing_player_pos = None
    lineup_slot = None

    def as_dict(self):
        dict = {
            "__bbref_pbp_in_game_substitution__": True,
            "inning": "{}".format(self.inning),
            'pbp_table_row_number': int(self.pbp_table_row_number),
            "sub_description": "{}".format(self.sub_description),
            "incoming_player_id_br": "{}".format(self.incoming_player_id_br),
            "incoming_player_pos": "{}".format(self.incoming_player_pos),
            "outgoing_player_id_br": "{}".format(self.outgoing_player_id_br),
            "outgoing_player_pos": "{}".format(self.outgoing_player_pos),
            'lineup_slot': int(self.lineup_slot)
        }
        return dict

    def display(self):
        display_dict(self.as_dict())

