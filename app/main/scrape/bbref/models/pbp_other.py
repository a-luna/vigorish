from dataclasses import dataclass


@dataclass
class BBRefPlayByPlayMiscEvent():
    """Information describing a challenged play or other game event."""

    inning_id: str = ""
    inning_label: str = ""
    pbp_table_row_number: str = "0"
    description: str = ""

    def as_dict(self):
        dict = {
            "__bbref_pbp_misc_event__": True,
            "inning_id": self.inning_id,
            "inning_label": self.inning_label,
            'pbp_table_row_number': int(self.pbp_table_row_number),
            "description": self.description
        }
        return dict
