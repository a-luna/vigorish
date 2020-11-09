from sqlalchemy import select

from vigorish.config.database import Pitch_Mix_By_Year_View, Player, Season
from vigorish.util.pitch_calcs import calc_pitch_mix


class AllPlayerData:
    def __init__(self, app, mlb_id):
        self.app = app
        self.db_engine = app["db_engine"]
        self.db_session = app["db_session"]
        self.mlb_id = mlb_id
        self.player = Player.find_by_mlb_id(self.db_session, self.mlb_id)

    def pitch_mix_by_year(self):
        s = select([Pitch_Mix_By_Year_View]).where(Pitch_Mix_By_Year_View.id == self.player.id)
        results = self.db_engine.execute(s).fetchall()
        pitch_mix = {}
        for d in [dict(row) for row in results]:
            season = self.db_session.query(Season).get(d["season_id"])
            pitch_mix[season.year] = calc_pitch_mix(d, d["total_pitches"])
        return pitch_mix

    def pitch_mix(self):
        return self.player.pitch_mix

    def pitch_mix_right(self):
        return self.player.pitch_mix_right

    def pitch_mix_left(self):
        return self.player.pitch_mix_left
