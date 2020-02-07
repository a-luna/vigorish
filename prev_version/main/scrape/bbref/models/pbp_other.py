from dataclasses import dataclass

from app.main.constants import PlayByPlayEventType


@dataclass
class BBRefPlayByPlayMiscEvent:
    """Information describing a challenged play or other game event."""

    inning_id: str = ""
    inning_label: str = ""
    pbp_table_row_number: str = "0"
    description: str = ""

    @property
    def event_type(self):
        return PlayByPlayEventType.MISC

    def as_dict(self):
        dict = {
            "__bbref_pbp_misc_event__": True,
            "inning_id": self.inning_id,
            "inning_label": self.inning_label,
            "pbp_table_row_number": int(self.pbp_table_row_number),
            "description": self.description,
        }
        return dict
