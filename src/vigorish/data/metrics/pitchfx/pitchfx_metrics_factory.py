from __future__ import annotations

from copy import deepcopy
from typing import Dict

import vigorish.database as db
from vigorish.data.metrics.pitchfx.pitchfx_batting_metrics import PitchFxBattingMetrics
from vigorish.data.metrics.pitchfx.pitchfx_pitching_metrics import PitchFxPitchingMetrics


class PitchFxMetricsFactory:
    def __init__(self, app):
        self.app = app
        self.db_session = app.db_session

    def for_pitcher_game(
        self, mlb_id: int, bbref_game_id: str, p_throws: str, remove_outliers: bool = False
    ) -> PitchFxPitchingMetrics:
        pitch_app_id = f"{bbref_game_id}_{mlb_id}"
        return self.for_pitch_app(pitch_app_id, p_throws, remove_outliers)

    def for_pitch_app(self, pitch_app_id: str, p_throws: str, remove_outliers: bool = False) -> PitchFxPitchingMetrics:
        pitch_app = db.PitchAppScrapeStatus.find_by_pitch_app_id(self.db_session, pitch_app_id)
        if not pitch_app:
            return None
        pfx = self.db_session.query(db.PitchFx).filter_by(pitch_app_db_id=pitch_app.id).all()
        return PitchFxPitchingMetrics(pfx, pitch_app.pitcher_id_mlb, p_throws, remove_outliers)

    def for_pitcher_season(
        self, mlb_id: int, year: int, p_throws: str, remove_outliers: bool = True
    ) -> PitchFxPitchingMetrics:
        pitcher = db.PlayerId.find_by_mlb_id(self.db_session, mlb_id)
        if not pitcher:
            return None
        season = db.Season.find_by_year(self.db_session, year)
        if not season:
            return None
        pfx = (
            self.db_session.query(db.PitchFx)
            .filter_by(pitcher_id=pitcher.db_player_id)
            .filter_by(season_id=season.id)
            .all()
        )
        return PitchFxPitchingMetrics(pfx, mlb_id, p_throws, remove_outliers)

    def for_pitcher_career(self, mlb_id: int, p_throws: str, remove_outliers: bool = True) -> PitchFxPitchingMetrics:
        pitcher = db.PlayerId.find_by_mlb_id(self.db_session, mlb_id)
        if not pitcher:
            return None
        pfx = self.db_session.query(db.PitchFx).filter_by(pitcher_id=pitcher.db_player_id).all()
        return PitchFxPitchingMetrics(pfx, mlb_id, p_throws, remove_outliers)

    def for_pitcher_by_year(
        self, mlb_id: int, p_throws: str, remove_outliers: bool = True
    ) -> Dict[int, PitchFxPitchingMetrics]:
        pitcher = db.PlayerId.find_by_mlb_id(self.db_session, mlb_id)
        if not pitcher:
            return None
        pfx = self.db_session.query(db.PitchFx).filter_by(pitcher_id=pitcher.db_player_id).all()
        season_ids = list({p.season_id for p in pfx})
        pfx_metrics_by_year = {}
        for sid in sorted(season_ids):
            season = self.db_session.query(db.Season).get(sid)
            pfx_for_season = list(filter(lambda x: x.season_id == sid, pfx))
            pfx_metrics_by_year[season.year] = PitchFxPitchingMetrics(
                deepcopy(pfx_for_season), mlb_id, p_throws, remove_outliers
            )
        return pfx_metrics_by_year

    def for_batter_game(self, mlb_id: int, bbref_game_id: str, remove_outliers: bool = False) -> PitchFxBattingMetrics:
        game_status = db.GameScrapeStatus.find_by_bbref_game_id(self.db_session, bbref_game_id)
        if not game_status:
            return None
        batter = db.PlayerId.find_by_mlb_id(self.db_session, mlb_id)
        if not batter:
            return None
        pfx = (
            self.db_session.query(db.PitchFx)
            .filter_by(game_status_id=game_status.id)
            .filter_by(batter_id=batter.db_player_id)
            .all()
        )
        return PitchFxBattingMetrics(pfx, mlb_id, remove_outliers)

    def for_batter_season(self, mlb_id: int, year: int, remove_outliers: bool = False) -> PitchFxBattingMetrics:
        season = db.Season.find_by_year(self.db_session, year)
        if not season:
            return None
        batter = db.PlayerId.find_by_mlb_id(self.db_session, mlb_id)
        if not batter:
            return None
        pfx = (
            self.db_session.query(db.PitchFx)
            .filter_by(batter_id=batter.db_player_id)
            .filter_by(season_id=season.id)
            .all()
        )
        return PitchFxBattingMetrics(pfx, mlb_id, remove_outliers)

    def for_batter_career(self, mlb_id: int, remove_outliers: bool = False) -> PitchFxBattingMetrics:
        batter = db.PlayerId.find_by_mlb_id(self.db_session, mlb_id)
        if not batter:
            return None
        pfx = self.db_session.query(db.PitchFx).filter_by(batter_id=batter.db_player_id).all()
        return PitchFxBattingMetrics(pfx, mlb_id, remove_outliers)

    def for_batter_by_year(self, mlb_id: int, remove_outliers: bool = False) -> Dict[int, PitchFxBattingMetrics]:
        batter = db.PlayerId.find_by_mlb_id(self.db_session, mlb_id)
        if not batter:
            return None
        pfx = self.db_session.query(db.PitchFx).filter_by(batter_id=batter.db_player_id).all()
        season_ids = list({p.season_id for p in pfx})
        pfx_metrics_by_year = {}
        for sid in season_ids:
            season = self.db_session.query(db.Season).get(sid)
            pfx_for_season = list(filter(lambda x: x.season_id == sid, pfx))
            pfx_metrics_by_year[season.year] = PitchFxBattingMetrics(pfx_for_season, mlb_id, remove_outliers)
        return pfx_metrics_by_year

    def for_team_pitching(self, team_id_br: str, year: int, remove_outliers: bool = True) -> PitchFxPitchingMetrics:
        team = db.Team.find_by_team_id_and_year(self.db_session, team_id_br, year)
        if not team:
            return None
        pfx = self.db_session.query(db.PitchFx).filter_by(team_pitching_id=team.id).all()
        return PitchFxPitchingMetrics(pfx, mlb_id=None, remove_outliers=remove_outliers)

    def for_team_batting(self, team_id_br: str, year: int, remove_outliers: bool = False) -> PitchFxPitchingMetrics:
        team = db.Team.find_by_team_id_and_year(self.db_session, team_id_br, year)
        if not team:
            return None
        pfx = self.db_session.query(db.PitchFx).filter_by(team_batting_id=team.id).all()
        return PitchFxBattingMetrics(pfx, mlb_id=None, remove_outliers=remove_outliers)
