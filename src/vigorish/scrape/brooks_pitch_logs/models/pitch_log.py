"""A single pitching appearance scraped from brooksbaseball.com."""
from vigorish.util.string_helpers import validate_brooks_game_id


class BrooksPitchLog:
    """A single pitching appearance scraped from brooksbaseball.com."""

    parsed_all_info = None
    pitcher_name = ""
    pitcher_id_mlb = 0
    pitch_app_id = ""
    total_pitch_count = 0
    pitch_count_by_inning = {}
    pitcher_team_id_bb = ""
    opponent_team_id_bb = ""
    bb_game_id = ""
    bbref_game_id = ""
    pitchfx_url = ""
    pitch_log_url = ""

    @property
    def game_date(self):
        result = validate_brooks_game_id(self.bb_game_id)
        if result.failure:
            return None
        game_dict = result.value
        return game_dict["game_date"]

    def as_dict(self):
        """Convert pitch log to a dictionary."""
        dict = {
            "__brooks_pitch_log__": True,
            "parsed_all_info": self.parsed_all_info,
            "pitcher_name": "{}".format(self.pitcher_name),
            "pitcher_id_mlb": self.pitcher_id_mlb,
            "pitch_app_id": "{}".format(self.pitch_app_id),
            "total_pitch_count": self.total_pitch_count,
            "pitch_count_by_inning": self.pitch_count_by_inning,
            "pitcher_team_id_bb": "{}".format(self.pitcher_team_id_bb),
            "opponent_team_id_bb": "{}".format(self.opponent_team_id_bb),
            "bb_game_id": "{}".format(self.bb_game_id),
            "bbref_game_id": "{}".format(self.bbref_game_id),
            "pitchfx_url": "{}".format(self.pitchfx_url),
            "pitch_log_url": "{}".format(self.pitch_log_url),
        }
        return dict
