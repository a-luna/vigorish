import json
from dataclasses import dataclass
from typing import Any

from vigorish.util.list_helpers import as_dict_list
from vigorish.util.string_helpers import get_brooks_team_id, validate_bbref_game_id


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
    def game_id_dict(self):
        return {f"{self.bbref_game_id}": f"{self.bb_game_id}"}

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
        return sum(
            int(pitch_stats.pitch_count) for pitch_stats in self.away_team_data.pitching_stats
        )

    @property
    def home_team_pitch_count(self):
        return sum(
            int(pitch_stats.pitch_count) for pitch_stats in self.home_team_data.pitching_stats
        )

    @property
    def pitch_count(self):
        return self.away_team_pitch_count + self.home_team_pitch_count

    @property
    def pitch_appearances(self):
        mlb_id_name_dict = {v: k for k, v in self.player_name_dict.items()}
        pitch_apps = self.away_team_data.pitching_stats.copy()
        pitch_apps.extend(self.home_team_data.pitching_stats.copy())
        pitch_apps_dicts = [pa.as_dict() for pa in pitch_apps]
        for app_dict in pitch_apps_dicts:
            player_id = app_dict["player_id_br"]
            app_dict["player_name"] = mlb_id_name_dict[player_id]
        return pitch_apps_dicts

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
            innings_list=as_dict_list(self.innings_list),
            player_id_match_log=self.player_id_match_log,
            umpires=as_dict_list(self.umpires),
            player_team_dict=self.player_team_dict,
            player_name_dict=self.player_name_dict,
        )

    def as_json(self):
        """Convert boxscore to JSON."""
        return json.dumps(self.as_dict(), indent=2, sort_keys=False)
