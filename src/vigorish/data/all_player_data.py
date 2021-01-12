from collections import defaultdict
from functools import lru_cache
from typing import Mapping, Union

from sqlalchemy import and_, select

from vigorish.database import (
    Pitch_Type_All_View,
    Pitch_Type_By_Year_View,
    Pitch_Type_Left_View,
    Pitch_Type_Right_View,
    Player,
    Season,
)
from vigorish.enums import PitchType
from vigorish.util.pitch_calcs import get_metrics_for_all_pitch_types

PitchMix = Mapping[PitchType, Mapping[str, Union[int, float]]]


class AllPlayerData:
    def __init__(self, app, mlb_id):
        self.app = app
        self.db_engine = self.app.db_engine
        self.db_session = self.app.db_session
        self.mlb_id = mlb_id
        self.player = Player.find_by_mlb_id(self.db_session, self.mlb_id)

    @property
    def seasons_played(self):
        return self.get_seasons_played()

    @property
    def years_played(self):
        return [s.year for s in self.seasons_played] if self.seasons_played else []

    @property
    def pitch_appearances(self):
        return self.player.pitch_apps

    @property
    def pitch_mix(self):
        return self.get_pitch_mix()

    @property
    def pitch_mix_right(self):
        return self.get_pitch_mix_right()

    @property
    def pitch_mix_left(self):
        return self.get_pitch_mix_left()

    @property
    def pitch_mix_by_year(self):
        return self.get_pitch_mix_by_year()

    @lru_cache(maxsize=None)
    def get_seasons_played(self):
        s = (
            select([Pitch_Type_By_Year_View.season_id])
            .where(Pitch_Type_By_Year_View.id == self.player.id)
            .distinct()
        )
        results = self.db_engine.execute(s).fetchall()
        seasons_played = []
        for d in [dict(row) for row in results]:
            season = self.db_session.query(Season).get(d["season_id"])
            seasons_played.append(season)
        return sorted(seasons_played, key=lambda x: x.year) if seasons_played else []

    @lru_cache(maxsize=None)
    def get_pitch_mix(self):
        s = select([Pitch_Type_All_View]).where(Pitch_Type_All_View.id == self.player.id)
        results = self.db_engine.execute(s).fetchall()
        return get_metrics_for_all_pitch_types(results)

    @lru_cache(maxsize=None)
    def get_pitch_mix_right(self):
        s = select([Pitch_Type_Right_View]).where(Pitch_Type_Right_View.id == self.player.id)
        results = self.db_engine.execute(s).fetchall()
        return get_metrics_for_all_pitch_types(results)

    @lru_cache(maxsize=None)
    def get_pitch_mix_left(self):
        s = select([Pitch_Type_Left_View]).where(Pitch_Type_Left_View.id == self.player.id)
        results = self.db_engine.execute(s).fetchall()
        return get_metrics_for_all_pitch_types(results)

    @lru_cache(maxsize=None)
    def get_pitch_mix_by_year(self):
        pitch_mix_by_year = defaultdict(dict)
        for season in self.seasons_played:
            s = select([Pitch_Type_By_Year_View]).where(
                and_(
                    Pitch_Type_By_Year_View.id == self.player.id,
                    Pitch_Type_By_Year_View.season_id == season.id,
                )
            )
            results = self.db_engine.execute(s).fetchall()
            pitch_mix_by_year[season.year] = get_metrics_for_all_pitch_types(results)
        return pitch_mix_by_year
