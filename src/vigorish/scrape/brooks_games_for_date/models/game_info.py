"""Individual game info scraped from brooksbaseball.com."""
from datetime import datetime

from dateutil import tz


class BrooksGameInfo:
    """Individual game info scraped from brooksbaseball.com."""

    might_be_postponed = None
    game_date_year = ""
    game_date_month = ""
    game_date_day = ""
    game_time_hour = ""
    game_time_minute = ""
    time_zone_name = ""
    bb_game_id = ""
    bbref_game_id = ""
    away_team_id_bb = ""
    home_team_id_bb = ""
    game_number_this_day = 0
    pitcher_appearance_count = 0
    pitcher_appearance_dict = {}

    @property
    def game_id_dict(self):
        return {f"{self.bbref_game_id}": f"{self.bb_game_id}"}

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

    @property
    def all_pitch_app_ids(self):
        return [
            f"{self.bbref_game_id}_{pitch_app_id}"
            for pitch_app_id in self.pitcher_appearance_dict.keys()
        ]

    def as_dict(self):
        """Convert pitcher appearance list/game log links to a dictionary."""
        return dict(
            __brooks_game_info__=True,
            might_be_postponed=self.might_be_postponed,
            game_date_year=int(self.game_date_year),
            game_date_month=int(self.game_date_month),
            game_date_day=int(self.game_date_day),
            game_time_hour=int(self.game_time_hour),
            game_time_minute=int(self.game_time_minute),
            time_zone_name=self.time_zone_name,
            bb_game_id=self.bb_game_id,
            bbref_game_id=self.bbref_game_id,
            away_team_id_bb=self.away_team_id_bb,
            home_team_id_bb=self.home_team_id_bb,
            game_number_this_day=self.game_number_this_day,
            pitcher_appearance_count=int(self.pitcher_appearance_count),
            pitcher_appearance_dict=self.pitcher_appearance_dict,
        )
