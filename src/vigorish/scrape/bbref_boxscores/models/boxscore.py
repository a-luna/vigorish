import json
from dataclasses import dataclass, field
from typing import Dict, List, Union

from vigorish.scrape.bbref_boxscores.models.boxscore_game_meta import BBRefBoxscoreMeta
from vigorish.scrape.bbref_boxscores.models.boxscore_team_data import BBRefBoxscoreTeamData
from vigorish.scrape.bbref_boxscores.models.half_inning import BBRefHalfInning
from vigorish.scrape.bbref_boxscores.models.umpire import BBRefUmpire
from vigorish.util.list_helpers import as_dict_list
from vigorish.util.string_helpers import get_brooks_team_id, validate_bbref_game_id


@dataclass
class BBRefBoxscore:
    """Batting and pitching statistics for a single MLB game."""

    boxscore_url: str
    bbref_game_id: str
    game_meta_info: BBRefBoxscoreMeta = None
    away_team_data: BBRefBoxscoreTeamData = None
    home_team_data: BBRefBoxscoreTeamData = None
    player_id_match_log: Dict[str, Union[str, int, List[str]]] = field(default_factory=dict)
    player_team_dict: Dict[str, str] = field(default_factory=dict)
    player_name_dict: Dict[str, str] = field(default_factory=dict)
    innings_list: List[BBRefHalfInning] = field(default_factory=list)
    umpires: List[BBRefUmpire] = field(default_factory=list)

    @property
    def bb_game_id(self):
        result = validate_bbref_game_id(self.bbref_game_id)
        game_date = result.value["game_date"]
        game_number = result.value["game_number"]
        away_team_id = get_brooks_team_id(self.away_team_data.team_id_br).lower()
        home_team_id = get_brooks_team_id(self.home_team_data.team_id_br).lower()
        return (
            f"gid_{game_date.year}_{game_date.month:02d}_{game_date.day:02d}_"
            f"{away_team_id}mlb_{home_team_id}mlb_{game_number}"
        )

    @property
    def game_date(self):
        result = validate_bbref_game_id(self.bbref_game_id)
        return result.value["game_date"] if result.success else None

    @property
    def away_team_pitch_appearance_count(self):
        return len(self.away_team_data.pitching_stats)

    @property
    def home_team_pitch_appearance_count(self):
        return len(self.home_team_data.pitching_stats)

    @property
    def pitch_appearance_count(self):
        return self.away_team_pitch_appearance_count + self.home_team_pitch_appearance_count

    @property
    def away_team_pitch_count(self):
        return sum(int(pitch_stats.pitch_count) for pitch_stats in self.away_team_data.pitching_stats)

    @property
    def home_team_pitch_count(self):
        return sum(int(pitch_stats.pitch_count) for pitch_stats in self.home_team_data.pitching_stats)

    @property
    def pitch_count(self):
        return self.away_team_pitch_count + self.home_team_pitch_count

    def __repr__(self):
        return f"<BBRefBoxscore bbref_game_id={self.bbref_game_id}>"

    def as_dict(self):
        """Convert boxscore to a dictionary."""
        return {
            "__bbref_boxscore__": True,
            "boxscore_url": self.boxscore_url,
            "bbref_game_id": self.bbref_game_id,
            "game_meta_info": self.game_meta_info.as_dict(),
            "away_team_data": self.away_team_data.as_dict(),
            "home_team_data": self.home_team_data.as_dict(),
            "innings_list": as_dict_list(self.innings_list),
            "player_id_match_log": self.player_id_match_log,
            "umpires": as_dict_list(self.umpires),
            "player_team_dict": self.player_team_dict,
            "player_name_dict": self.player_name_dict,
        }

    def as_json(self):
        """Convert boxscore to JSON."""
        return json.dumps(self.as_dict(), indent=2, sort_keys=False)
