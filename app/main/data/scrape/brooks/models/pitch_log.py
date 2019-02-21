"""A single pitching appearance scraped from brooksbaseball.com."""

class BrooksPitchLog():
    """A single pitching appearance scraped from brooksbaseball.com."""

    scrape_success = ""
    scrape_error = ""
    gamelog_url = ""
    pitchfx_url = ""
    pitcher_id_mlb = ""
    pitch_count_by_inning = {}
    total_pitch_count = ""
    pitcher_name = ""
    bb_game_id = ""
    pitch_app_guid = ""

    def as_dict(self):
        """Convert pitch log to a dictionary."""
        dict = {
            "scrape_success": "{}".format(self.scrape_success),
            "scrape_error": "{}".format(self.scrape_error),
            "pitcher_name": "{}".format(self.pitcher_name),
            "pitcher_id_mlb": "{}".format(self.pitcher_id_mlb),
            "total_pitch_count": int(self.total_pitch_count),
            "pitch_count_by_inning": self.pitch_count_by_inning,
            "pitch_app_guid": "{}".format(self.pitch_app_guid),
            "bb_game_id": "{}".format(self.bb_game_id),
            "pitchfx_url": "{}".format(self.pitchfx_url),
            "gamelog_url": "{}".format(self.gamelog_url)
        }
        return dict
