"""A single pitching appearance scraped from brooksbaseball.com."""
from datetime import datetime

from dateutil import tz


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
    game_date_year = ""
    game_date_month = ""
    game_date_day = ""
    game_time_hour = ""
    game_time_minute = ""
    time_zone_name = ""
    pitchfx_url = ""
    pitch_log_url = ""

    @property
    def game_date(self):
        return datetime(self.game_date_year, self.game_date_month, self.game_date_day).date()

    @property
    def game_start_time(self):
        if not self.game_time_hour:
            return None
        game_start_time = datetime(
            year=self.game_date.year,
            month=self.game_date.month,
            day=self.game_date.day,
            hour=self.game_time_hour,
            minute=self.game_time_minute,
        )
        return game_start_time.replace(tzinfo=tz.gettz(self.time_zone_name))

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
            "game_date_year": int(self.game_date_year),
            "game_date_month": int(self.game_date_month),
            "game_date_day": int(self.game_date_day),
            "game_time_hour": int(self.game_time_hour),
            "game_time_minute": int(self.game_time_minute),
            "time_zone_name": self.time_zone_name,
            "pitchfx_url": "{}".format(self.pitchfx_url),
            "pitch_log_url": "{}".format(self.pitch_log_url),
        }
        return dict
