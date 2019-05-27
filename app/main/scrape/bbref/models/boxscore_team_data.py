from dataclasses import dataclass
from typing import Any


@dataclass
class BBRefBoxscoreTeamData:
    starting_lineup: Any
    batting_stats: Any
    pitching_stats: Any
    team_id_br: str = ""
    total_wins_before_game: str = "0"
    total_losses_before_game: str = "0"
    total_runs_scored_by_team: str = "0"
    total_runs_scored_by_opponent: str = "0"
    total_hits_by_team: str = "0"
    total_hits_by_opponent: str = "0"
    total_errors_by_team: str = "0"
    total_errors_by_opponent: str = "0"

    def as_dict(self):
        return dict(
            __bbref_boxscore_team_data__=True,
            team_id_br=self.team_id_br,
            total_wins_before_game=int(self.total_wins_before_game),
            total_losses_before_game=int(self.total_losses_before_game),
            total_runs_scored_by_team=int(self.total_runs_scored_by_team),
            total_runs_scored_by_opponent=int(self.total_runs_scored_by_opponent),
            total_hits_by_team=int(self.total_hits_by_team),
            total_hits_by_opponent=int(self.total_hits_by_opponent),
            total_errors_by_team=int(self.total_errors_by_team),
            total_errors_by_opponent=int(self.total_errors_by_opponent),
            starting_lineup=self._flatten(self.starting_lineup),
            batting_stats=self._flatten(self.batting_stats),
            pitching_stats=self._flatten(self.pitching_stats),
        )

    @staticmethod
    def _flatten(objects):
        return [obj.as_dict() for obj in objects]
