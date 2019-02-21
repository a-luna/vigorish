"""Individual game info scraped from brooksbaseball.com."""

class BrooksGameInfo():
    """Individual game info scraped from brooksbaseball.com."""

    game_start_time = None
    game_date_year = ""
    game_date_month = ""
    game_date_day = ""
    game_time_hour = ""
    game_time_minute = ""
    time_zone_name = ""
    bb_game_id = ""
    away_team_id_bb = ""
    home_team_id_bb = ""
    game_number_this_day = ""
    pitcher_appearance_count = ""
    pitcher_appearance_dict = {}

    def as_dict(self):
        """Convert pitcher appearance list/game log links to a dictionary."""
        dict = {
            "__brooks_game_info__": True,
            "game_date_year": int(self.game_date_year),
            "game_date_month": int(self.game_date_month),
            "game_date_day": int(self.game_date_day),
            "game_time_hour": int(self.game_time_hour),
            "game_time_minute": int(self.game_time_minute),
            "time_zone_name": "{}".format(self.time_zone_name),
            "bb_game_id": "{}".format(self.bb_game_id),
            "away_team_id_bb": "{}".format(self.away_team_id_bb),
            "home_team_id_bb": "{}".format(self.home_team_id_bb),
            "game_number_this_day": int(self.game_number_this_day),
            "pitcher_appearance_count": int(self.pitcher_appearance_count),
            "pitcher_appearance_dict": self.pitcher_appearance_dict
        }
        return dict
