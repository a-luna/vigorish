import json

class BBRefBoxScore():
    """Batting and pitching statistics for a single MLB game."""

    scrape_success = ""
    scrape_error = ""
    url = ""
    bbref_game_id = ""
    away_team_id_br = ""
    home_team_id_br = ""
    away_team_runs = ""
    home_team_runs = ""
    away_team_wins_before_game = ""
    away_team_losses_before_game = ""
    home_team_wins_before_game = ""
    home_team_losses_before_game = ""
    attendance = ""
    park_name = ""
    game_duration = ""
    day_night = ""
    field_type = ""
    first_pitch_temperature = ""
    first_pitch_wind = ""
    first_pitch_clouds = ""
    first_pitch_precipitation = ""
    away_team_linescore_innings = []
    away_team_linescore_totals = None
    home_team_linescore_innings = []
    home_team_linescore_totals = None
    batting_stats = []
    pitching_stats = []
    umpires = []
    away_starting_lineup = []
    home_starting_lineup = []
    inning_summaries = []
    play_by_play = []
    player_dict = {}

    def as_dict(self):
        """Convert boxscore to a dictionary."""
        dict = {
            "scrape_success": "{}".format(self.scrape_success),
            "scrape_error": "{}".format(self.scrape_error),
            "url": "{}".format(self.url),
            "bbref_game_id": "{}".format(self.bbref_game_id),
            "away_team_id_br": "{}".format(self.away_team_id_br),
            "home_team_id_br": "{}".format(self.home_team_id_br),
            "away_team_runs": int(self.away_team_runs),
            "home_team_runs": int(self.home_team_runs),
            "away_team_wins_before_game": int(self.away_team_wins_before_game),
            "away_team_losses_before_game": int(self.away_team_losses_before_game),
            "home_team_wins_before_game": int(self.home_team_wins_before_game),
            "home_team_losses_before_game": int(self.home_team_losses_before_game),
            "attendance": int(self.attendance),
            "park_name": "{}".format(self.park_name),
            "game_duration": "{}".format(self.game_duration),
            "day_night": "{}".format(self.day_night),
            "field_type": "{}".format(self.field_type),
            "first_pitch_temperature": int(self.first_pitch_temperature),
            "first_pitch_wind": "{}".format(self.first_pitch_wind),
            "first_pitch_clouds": "{}".format(self.first_pitch_clouds),
            "first_pitch_precipitation": "{}".format(self.first_pitch_precipitation),
            "away_team_linescore_innings": self._flatten(self.away_team_linescore_innings),
            "away_team_linescore_totals": self.away_team_linescore_totals.as_dict(),
            "home_team_linescore_innings": self._flatten(self.home_team_linescore_innings),
            "home_team_linescore_totals": self.home_team_linescore_totals.as_dict(),
            "batting_stats": self._flatten(self.batting_stats),
            "pitching_stats": self._flatten(self.pitching_stats),
            "umpires": self._flatten(self.umpires),
            "away_starting_lineup": self._flatten(self.away_starting_lineup),
            "home_starting_lineup": self._flatten(self.home_starting_lineup),
            "inning_summaries": self.inning_summaries,
            "play_by_play": self._flatten(self.play_by_play),
            "player_dict": self.player_dict
        }
        return dict

    def as_json(self):
        """Convert boxscore to JSON."""
        return json.dumps(self.as_dict(), indent=2, sort_keys=True)

    @staticmethod
    def _flatten(objects):
        return [obj.as_dict() for obj in objects]
