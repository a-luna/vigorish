import json

from app.main.scrape.bbref.models.boxscore_game_meta import BBRefBoxscoreMeta
from app.main.scrape.bbref.models.boxscore_team_data import BBRefBoxscoreTeamData

from app.main.constants import TEAM_ID_DICT
from app.main.util.string_functions import validate_bbref_game_id


class BBRefBoxscore:
    """Batting and pitching statistics for a single MLB game."""

    def __init__(self, url=None):
        self.boxscore_url = url
        self.bbref_game_id = ""
        self.game_meta_info = BBRefBoxscoreMeta()
        self.away_team_data = None
        self.home_team_data = None
        self.innings_list = []
        self.player_id_match_log = []
        self.umpires = []
        self.player_team_dict = {}
        self.player_name_dict = {}

    @property
    def upload_id(self):
        return self.bbref_game_id

    def as_dict(self):
        """Convert boxscore to a dictionary."""
        boxscore_dict = {
            "__bbref_boxscore__": True,
            "boxscore_url": "{}".format(self.boxscore_url),
            "bbref_game_id": "{}".format(self.bbref_game_id),
            "game_meta_info": self.game_meta_info.as_dict(),
            "away_team_data": self.away_team_data.as_dict(),
            "home_team_data": self.home_team_data.as_dict(),
            "innings_list": self._flatten(self.innings_list),
            "player_id_match_log": self.player_id_match_log,
            "umpires": self._flatten(self.umpires),
            "player_team_dict": self.player_team_dict,
            "player_name_dict": self.player_name_dict,
        }
        return boxscore_dict

    def get_game_id_dict(self):
        result = validate_bbref_game_id(self.bbref_game_id)
        game_date = result.value["game_date"]
        game_number = result.value["game_number"]
        away_team_id = self.__get_bb_team_id(self.away_team_data.team_id_br).lower()
        home_team_id = self.__get_bb_team_id(self.home_team_data.team_id_br).lower()
        bb_game_id = f"gid_{game_date.year}_{game_date.month:02d}_{game_date.day:02d}_{away_team_id}mlb_{home_team_id}mlb_{game_number}"
        return {f"{self.bbref_game_id}": f"{bb_game_id}"}

    def get_game_date(self):
        result = validate_bbref_game_id(self.bbref_game_id)
        return result.value["game_date"]

    @staticmethod
    def __get_bb_team_id(br_team_id):
        if br_team_id in TEAM_ID_DICT:
            return TEAM_ID_DICT[br_team_id]
        return br_team_id

    def as_json(self):
        """Convert boxscore to JSON."""
        return json.dumps(self.as_dict(), indent=2, sort_keys=False)

    @staticmethod
    def _flatten(objects):
        return [obj.as_dict() for obj in objects]
