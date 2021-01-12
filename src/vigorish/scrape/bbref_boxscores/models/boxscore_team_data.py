from dataclasses import dataclass, field
from typing import List

from vigorish.scrape.bbref_boxscores.models.bat_stats import BBRefBatStats
from vigorish.scrape.bbref_boxscores.models.pitch_stats import BBRefPitchStats
from vigorish.scrape.bbref_boxscores.models.starting_lineup_slot import BBRefStartingLineupSlot
from vigorish.util.list_helpers import as_dict_list


@dataclass
class BBRefBoxscoreTeamData:
    team_id_br: str = ""
    total_wins_before_game: int = 0
    total_losses_before_game: int = 0
    total_runs_scored_by_team: int = 0
    total_runs_scored_by_opponent: int = 0
    total_hits_by_team: int = 0
    total_hits_by_opponent: int = 0
    total_errors_by_team: int = 0
    total_errors_by_opponent: int = 0
    team_won: bool = field(init=False)
    pitcher_of_record: str = ""
    pitcher_earned_save: str = ""
    starting_lineup: List[BBRefStartingLineupSlot] = field(default_factory=list)
    batting_stats: List[BBRefBatStats] = field(default_factory=list)
    pitching_stats: List[BBRefPitchStats] = field(default_factory=list)

    def __post_init__(self):
        self.team_won = self.total_runs_scored_by_team > self.total_runs_scored_by_opponent

    def as_dict(self):
        return {
            "__bbref_boxscore_team_data__": True,
            "team_id_br": self.team_id_br,
            "total_wins_before_game": self.total_wins_before_game,
            "total_losses_before_game": self.total_losses_before_game,
            "total_runs_scored_by_team": self.total_runs_scored_by_team,
            "total_runs_scored_by_opponent": self.total_runs_scored_by_opponent,
            "total_hits_by_team": self.total_hits_by_team,
            "total_hits_by_opponent": self.total_hits_by_opponent,
            "total_errors_by_team": self.total_errors_by_team,
            "total_errors_by_opponent": self.total_errors_by_opponent,
            "team_won": self.team_won,
            "pitcher_of_record": self.pitcher_of_record,
            "pitcher_earned_save": self.pitcher_earned_save,
            "starting_lineup": as_dict_list(self.starting_lineup),
            "batting_stats": as_dict_list(self.batting_stats),
            "pitching_stats": as_dict_list(self.pitching_stats),
        }
