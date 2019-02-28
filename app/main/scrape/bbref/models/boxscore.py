import json

from app.main.scrape.bbref.models.boxscore_game_meta import BBRefBoxscoreMeta
from app.main.scrape.bbref.models.boxscore_team_data import BBRefBoxscoreTeamData

class BBRefBoxscore():
    """Batting and pitching statistics for a single MLB game."""

    boxscore_url = ""
    bbref_game_id = ""
    game_meta_info = BBRefBoxscoreMeta()
    away_team_data = BBRefBoxscoreTeamData()
    home_team_data = BBRefBoxscoreTeamData()
    innings_list = []
    player_id_match_log = {}
    umpires = []
    player_team_dict = {}
    player_name_dict = {}

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
            "player_name_dict": self.player_name_dict
        }
        return boxscore_dict

    def as_json(self):
        """Convert boxscore to JSON."""
        return json.dumps(self.as_dict(), indent=2, sort_keys=False)

    @staticmethod
    def _flatten(objects):
        return [obj.as_dict() for obj in objects]
