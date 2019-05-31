import json
from dataclasses import dataclass
from typing import Any

from app.main.constants import TEAM_ID_DICT
from app.main.scrape.bbref.models.boxscore_game_meta import BBRefBoxscoreMeta
from app.main.scrape.bbref.models.boxscore_team_data import BBRefBoxscoreTeamData
from app.main.util.string_functions import validate_bbref_game_id


@dataclass
class BBRefBoxscore:
    """Batting and pitching statistics for a single MLB game."""

    boxscore_url: str
    bbref_game_id: str
    game_meta_info: Any
    away_team_data: Any
    home_team_data: Any
    innings_list: Any
    umpires: Any
    player_id_match_log: Any
    player_team_dict: Any
    player_name_dict: Any

    @property
    def upload_id(self):
        return self.bbref_game_id

    @property
    def brooks_game_id(self):
        result = validate_bbref_game_id(self.bbref_game_id)
        game_date = result.value["game_date"]
        game_number = result.value["game_number"]
        away_team_id = self._get_brooks_team_id(self.away_team_data.team_id_br).lower()
        home_team_id = self._get_brooks_team_id(self.home_team_data.team_id_br).lower()
        return f"gid_{game_date.year}_{game_date.month:02d}_{game_date.day:02d}_{away_team_id}mlb_{home_team_id}mlb_{game_number}"

    @property
    def game_id_dict(self):
        return {f"{self.bbref_game_id}": f"{self.brooks_game_id}"}

    @property
    def game_date(self):
        result = validate_bbref_game_id(self.bbref_game_id)
        return result.value["game_date"]

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
        return dict(
            __bbref_boxscore__=True,
            boxscore_url=self.boxscore_url,
            bbref_game_id=self.bbref_game_id,
            game_meta_info=self.game_meta_info.as_dict(),
            away_team_data=self.away_team_data.as_dict(),
            home_team_data=self.home_team_data.as_dict(),
            innings_list=self._flatten(self.innings_list),
            player_id_match_log=self.player_id_match_log,
            umpires=self._flatten(self.umpires),
            player_team_dict=self.player_team_dict,
            player_name_dict=self.player_name_dict)

    def as_json(self):
        """Convert boxscore to JSON."""
        return json.dumps(self.as_dict(), indent=2, sort_keys=False)

    @staticmethod
    def _get_brooks_team_id(br_team_id):
        if br_team_id in TEAM_ID_DICT:
            return TEAM_ID_DICT[br_team_id]
        return br_team_id

    @staticmethod
    def _flatten(objects):
        return [obj.as_dict() for obj in objects]
