from collections import defaultdict
from typing import Mapping, Union

from sqlalchemy import select

from vigorish.config.database import (
    Pitch_Mix_By_Year_View,
    Pitch_Type_All_View,
    Pitch_Type_By_Year_View,
    Player,
    Season,
)
from vigorish.enums import PitchType
from vigorish.util.pitch_calcs import calc_pitch_mix, get_pitch_type_metrics

PitchMix = Mapping[PitchType, Mapping[str, Union[int, float]]]


class AllPlayerData:
    def __init__(self, app, mlb_id):
        self.app = app
        self.db_engine = self.app.db_engine
        self.db_session = self.app.db_session
        self.mlb_id = mlb_id
        self.player = Player.find_by_mlb_id(self.db_session, self.mlb_id)

    @property
    def years_played(self):
        s = select([Pitch_Mix_By_Year_View]).where(Pitch_Mix_By_Year_View.id == self.player.id)
        results = self.db_engine.execute(s).fetchall()
        years_played = []
        for d in [dict(row) for row in results]:
            season = self.db_session.query(Season).get(d["season_id"])
            years_played.append(season.year)
        return sorted(years_played)

    @property
    def pitch_mix(self):
        return self.player.pitch_mix

    @property
    def pitch_mix_right(self):
        return self.player.pitch_mix_right

    @property
    def pitch_mix_left(self):
        return self.player.pitch_mix_left

    @property
    def pitch_mix_by_year(self):
        s = select([Pitch_Mix_By_Year_View]).where(Pitch_Mix_By_Year_View.id == self.player.id)
        results = self.db_engine.execute(s).fetchall()
        pitch_mix = {}
        for d in [dict(row) for row in results]:
            season = self.db_session.query(Season).get(d["season_id"])
            pitch_mix[season.year] = calc_pitch_mix(d)
        return pitch_mix

    @property
    def pitch_appearances(self):
        return self.player.pitch_apps

    @property
    def pitch_types(self):
        s = select([Pitch_Type_All_View]).where(Pitch_Type_All_View.id == self.player.id)
        results = self.db_engine.execute(s).fetchall()
        pitch_types = {}
        for d in [dict(row) for row in results]:
            (pitch_type, metrics) = get_pitch_type_metrics(d)
            pitch_types[pitch_type] = metrics
        return pitch_types

    @property
    def pitch_types_by_year(self):
        s = select([Pitch_Type_By_Year_View]).where(Pitch_Type_By_Year_View.id == self.player.id)
        results = self.db_engine.execute(s).fetchall()
        pitch_types = defaultdict(dict)
        for d in [dict(row) for row in results]:
            season = self.db_session.query(Season).get(d["season_id"])
            (pitch_type, metrics) = get_pitch_type_metrics(d)
            pitch_types[season.year][pitch_type] = metrics
        return pitch_types
