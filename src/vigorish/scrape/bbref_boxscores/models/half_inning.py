from dataclasses import dataclass, field
from typing import List

from vigorish.scrape.bbref_boxscores.models.pbp_event import BBRefPlayByPlayEvent
from vigorish.scrape.bbref_boxscores.models.pbp_other import BBRefPlayByPlayMiscEvent
from vigorish.scrape.bbref_boxscores.models.pbp_substitution import BBRefInGameSubstitution
from vigorish.util.list_helpers import as_dict_list


@dataclass
class BBRefHalfInning:
    inning_id: str = ""
    inning_label: str = ""
    begin_inning_summary: str = ""
    end_inning_summary: str = ""
    inning_total_runs: str = "0"
    inning_total_hits: str = "0"
    inning_total_errors: str = "0"
    inning_total_left_on_base: str = "0"
    away_team_runs_after_inning: str = "0"
    home_team_runs_after_inning: str = "0"
    game_events: List[BBRefPlayByPlayEvent] = field(default_factory=list)
    substitutions: List[BBRefInGameSubstitution] = field(default_factory=list)
    misc_events: List[BBRefPlayByPlayMiscEvent] = field(default_factory=list)

    def as_dict(self):
        """Convert half-inning to a dictionary."""
        return {
            "__bbref_boxscore_half_inning__": True,
            "inning_id": self.inning_id,
            "inning_label": self.inning_label,
            "begin_inning_summary": self.begin_inning_summary,
            "end_inning_summary": self.end_inning_summary,
            "inning_total_runs": int(self.inning_total_runs),
            "inning_total_hits": int(self.inning_total_hits),
            "inning_total_errors": int(self.inning_total_errors),
            "inning_total_left_on_base": int(self.inning_total_left_on_base),
            "away_team_runs_after_inning": int(self.away_team_runs_after_inning),
            "home_team_runs_after_inning": int(self.home_team_runs_after_inning),
            "game_events": as_dict_list(self.game_events),
            "substitutions": as_dict_list(self.substitutions),
            "misc_events": as_dict_list(self.misc_events),
        }
