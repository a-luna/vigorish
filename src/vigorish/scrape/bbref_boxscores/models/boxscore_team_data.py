from dataclasses import dataclass
from typing import Any

from vigorish.util.list_helpers import as_dict_list


@dataclass
class BBRefBoxscoreTeamData:
    starting_lineup: Any
    batting_stats: Any
    pitching_stats: Any
    team_id_br: str = ""
    total_wins_before_game: int = 0
    total_losses_before_game: int = 0
    total_runs_scored_by_team: int = 0
    total_runs_scored_by_opponent: int = 0
    total_hits_by_team: int = 0
    total_hits_by_opponent: int = 0
    total_errors_by_team: int = 0
    total_errors_by_opponent: int = 0

    def as_dict(self):
        return dict(
            __bbref_boxscore_team_data__=True,
            team_id_br=self.team_id_br,
            total_wins_before_game=self.total_wins_before_game,
            total_losses_before_game=self.total_losses_before_game,
            total_runs_scored_by_team=self.total_runs_scored_by_team,
            total_runs_scored_by_opponent=self.total_runs_scored_by_opponent,
            total_hits_by_team=self.total_hits_by_team,
            total_hits_by_opponent=self.total_hits_by_opponent,
            total_errors_by_team=self.total_errors_by_team,
            total_errors_by_opponent=self.total_errors_by_opponent,
            starting_lineup=as_dict_list(self.starting_lineup),
            batting_stats=as_dict_list(self.batting_stats),
            pitching_stats=as_dict_list(self.pitching_stats),
        )
