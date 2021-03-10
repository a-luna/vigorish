from sqlalchemy import and_, func, join, select
from sqlalchemy_utils.view import create_view

import vigorish.database as db
from vigorish.data.metrics import PitchStatsMetrics
from vigorish.views.pitch_stats_col_expressions import (
    bb_per_nine,
    bb_rate,
    era,
    hr_per_fb,
    hr_per_nine,
    innings_pitched,
    k_minus_bb,
    k_per_bb,
    k_per_nine,
    k_rate,
    whip,
)


class PitchStats_All_View(db.Base):
    __table__ = create_view(
        name="pitchstats_all",
        selectable=select(
            [
                db.PitchStats.player_id.label("id"),
                db.PitchStats.player_id_mlb.label("mlb_id"),
                db.PitchStats.player_id_bbref.label("bbref_id"),
                func.count(db.PitchStats.id).label("total_games"),
                func.sum(db.PitchStats.is_sp).label("games_as_sp"),
                func.sum(db.PitchStats.is_rp).label("games_as_rp"),
                func.sum(db.PitchStats.is_wp).label("wins"),
                func.sum(db.PitchStats.is_lp).label("losses"),
                func.sum(db.PitchStats.is_sv).label("saves"),
                innings_pitched,
                func.sum(db.PitchStats.total_outs).label("total_outs"),
                func.sum(db.PitchStats.batters_faced).label("batters_faced"),
                func.sum(db.PitchStats.runs).label("runs"),
                func.sum(db.PitchStats.earned_runs).label("earned_runs"),
                func.sum(db.PitchStats.hits).label("hits"),
                func.sum(db.PitchStats.homeruns).label("homeruns"),
                func.sum(db.PitchStats.strikeouts).label("strikeouts"),
                func.sum(db.PitchStats.bases_on_balls).label("bases_on_balls"),
                era,
                whip,
                k_per_nine,
                bb_per_nine,
                hr_per_nine,
                k_per_bb,
                k_rate,
                bb_rate,
                k_minus_bb,
                hr_per_fb,
                func.sum(db.PitchStats.pitch_count).label("pitch_count"),
                func.sum(db.PitchStats.strikes).label("strikes"),
                func.sum(db.PitchStats.strikes_contact).label("strikes_contact"),
                func.sum(db.PitchStats.strikes_swinging).label("strikes_swinging"),
                func.sum(db.PitchStats.strikes_looking).label("strikes_looking"),
                func.sum(db.PitchStats.ground_balls).label("ground_balls"),
                func.sum(db.PitchStats.fly_balls).label("fly_balls"),
                func.sum(db.PitchStats.line_drives).label("line_drives"),
                func.sum(db.PitchStats.unknown_type).label("unknown_type"),
                func.sum(db.PitchStats.inherited_runners).label("inherited_runners"),
                func.sum(db.PitchStats.inherited_scored).label("inherited_scored"),
                func.sum(db.PitchStats.wpa_pitch).label("wpa_pitch"),
                func.sum(db.PitchStats.re24_pitch).label("re24_pitch"),
            ]
        )
        .select_from(db.PitchStats)
        .group_by(db.PitchStats.player_id)
        .order_by(db.PitchStats.player_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pitch_stats_for_career_for_player(cls, db_engine, mlb_id):
        s = select([cls]).where(cls.mlb_id == mlb_id)
        results = db_engine.execute(s).fetchall()
        return PitchStatsMetrics.from_query_results(results)[0] if results else None


class PitchStats_SP_View(db.Base):
    __table__ = create_view(
        name="pitchstats_as_sp",
        selectable=select(
            [
                db.PitchStats.player_id.label("id"),
                db.PitchStats.player_id_mlb.label("mlb_id"),
                db.PitchStats.player_id_bbref.label("bbref_id"),
                func.count(db.PitchStats.id).label("total_games"),
                func.sum(db.PitchStats.is_sp).label("games_as_sp"),
                func.sum(db.PitchStats.is_rp).label("games_as_rp"),
                func.sum(db.PitchStats.is_wp).label("wins"),
                func.sum(db.PitchStats.is_lp).label("losses"),
                func.sum(db.PitchStats.is_sv).label("saves"),
                innings_pitched,
                func.sum(db.PitchStats.total_outs).label("total_outs"),
                func.sum(db.PitchStats.batters_faced).label("batters_faced"),
                func.sum(db.PitchStats.runs).label("runs"),
                func.sum(db.PitchStats.earned_runs).label("earned_runs"),
                func.sum(db.PitchStats.hits).label("hits"),
                func.sum(db.PitchStats.homeruns).label("homeruns"),
                func.sum(db.PitchStats.strikeouts).label("strikeouts"),
                func.sum(db.PitchStats.bases_on_balls).label("bases_on_balls"),
                era,
                whip,
                k_per_nine,
                bb_per_nine,
                hr_per_nine,
                func.avg(db.PitchStats.game_score).label("avg_game_score"),
                k_per_bb,
                k_rate,
                bb_rate,
                k_minus_bb,
                hr_per_fb,
                func.sum(db.PitchStats.pitch_count).label("pitch_count"),
                func.sum(db.PitchStats.strikes).label("strikes"),
                func.sum(db.PitchStats.strikes_contact).label("strikes_contact"),
                func.sum(db.PitchStats.strikes_swinging).label("strikes_swinging"),
                func.sum(db.PitchStats.strikes_looking).label("strikes_looking"),
                func.sum(db.PitchStats.ground_balls).label("ground_balls"),
                func.sum(db.PitchStats.fly_balls).label("fly_balls"),
                func.sum(db.PitchStats.line_drives).label("line_drives"),
                func.sum(db.PitchStats.unknown_type).label("unknown_type"),
                func.sum(db.PitchStats.inherited_runners).label("inherited_runners"),
                func.sum(db.PitchStats.inherited_scored).label("inherited_scored"),
                func.sum(db.PitchStats.wpa_pitch).label("wpa_pitch"),
                func.sum(db.PitchStats.re24_pitch).label("re24_pitch"),
            ]
        )
        .where(db.PitchStats.is_sp == 1)
        .select_from(db.PitchStats)
        .group_by(db.PitchStats.player_id)
        .order_by(db.PitchStats.player_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pitch_stats_as_sp_for_player(cls, db_engine, mlb_id):
        s = select([cls]).where(cls.mlb_id == mlb_id)
        results = db_engine.execute(s).fetchall()
        return PitchStatsMetrics.from_query_results(results)[0] if results else None


class PitchStats_RP_View(db.Base):
    __table__ = create_view(
        name="pitchstats_as_rp",
        selectable=select(
            [
                db.PitchStats.player_id.label("id"),
                db.PitchStats.player_id_mlb.label("mlb_id"),
                db.PitchStats.player_id_bbref.label("bbref_id"),
                func.count(db.PitchStats.id).label("total_games"),
                func.sum(db.PitchStats.is_sp).label("games_as_sp"),
                func.sum(db.PitchStats.is_rp).label("games_as_rp"),
                func.sum(db.PitchStats.is_wp).label("wins"),
                func.sum(db.PitchStats.is_lp).label("losses"),
                func.sum(db.PitchStats.is_sv).label("saves"),
                innings_pitched,
                func.sum(db.PitchStats.total_outs).label("total_outs"),
                func.sum(db.PitchStats.batters_faced).label("batters_faced"),
                func.sum(db.PitchStats.runs).label("runs"),
                func.sum(db.PitchStats.earned_runs).label("earned_runs"),
                func.sum(db.PitchStats.hits).label("hits"),
                func.sum(db.PitchStats.homeruns).label("homeruns"),
                func.sum(db.PitchStats.strikeouts).label("strikeouts"),
                func.sum(db.PitchStats.bases_on_balls).label("bases_on_balls"),
                era,
                whip,
                k_per_nine,
                bb_per_nine,
                hr_per_nine,
                k_per_bb,
                k_rate,
                bb_rate,
                k_minus_bb,
                hr_per_fb,
                func.sum(db.PitchStats.pitch_count).label("pitch_count"),
                func.sum(db.PitchStats.strikes).label("strikes"),
                func.sum(db.PitchStats.strikes_contact).label("strikes_contact"),
                func.sum(db.PitchStats.strikes_swinging).label("strikes_swinging"),
                func.sum(db.PitchStats.strikes_looking).label("strikes_looking"),
                func.sum(db.PitchStats.ground_balls).label("ground_balls"),
                func.sum(db.PitchStats.fly_balls).label("fly_balls"),
                func.sum(db.PitchStats.line_drives).label("line_drives"),
                func.sum(db.PitchStats.unknown_type).label("unknown_type"),
                func.sum(db.PitchStats.inherited_runners).label("inherited_runners"),
                func.sum(db.PitchStats.inherited_scored).label("inherited_scored"),
                func.sum(db.PitchStats.wpa_pitch).label("wpa_pitch"),
                func.sum(db.PitchStats.re24_pitch).label("re24_pitch"),
            ]
        )
        .where(db.PitchStats.is_rp == 1)
        .select_from(db.PitchStats)
        .group_by(db.PitchStats.player_id)
        .order_by(db.PitchStats.player_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pitch_stats_as_rp_for_player(cls, db_engine, mlb_id):
        s = select([cls]).where(cls.mlb_id == mlb_id)
        results = db_engine.execute(s).fetchall()
        return PitchStatsMetrics.from_query_results(results)[0] if results else None


class PitchStats_By_Year_View(db.Base):
    __table__ = create_view(
        name="pitchstats_by_year",
        selectable=select(
            [
                db.PitchStats.player_id.label("id"),
                db.PitchStats.player_id_mlb.label("mlb_id"),
                db.PitchStats.player_id_bbref.label("bbref_id"),
                db.PitchStats.season_id.label("season_id"),
                db.Season.year.label("year"),
                func.count(db.PitchStats.id).label("total_games"),
                func.sum(db.PitchStats.is_sp).label("games_as_sp"),
                func.sum(db.PitchStats.is_rp).label("games_as_rp"),
                func.sum(db.PitchStats.is_wp).label("wins"),
                func.sum(db.PitchStats.is_lp).label("losses"),
                func.sum(db.PitchStats.is_sv).label("saves"),
                innings_pitched,
                func.sum(db.PitchStats.total_outs).label("total_outs"),
                func.sum(db.PitchStats.batters_faced).label("batters_faced"),
                func.sum(db.PitchStats.runs).label("runs"),
                func.sum(db.PitchStats.earned_runs).label("earned_runs"),
                func.sum(db.PitchStats.hits).label("hits"),
                func.sum(db.PitchStats.homeruns).label("homeruns"),
                func.sum(db.PitchStats.strikeouts).label("strikeouts"),
                func.sum(db.PitchStats.bases_on_balls).label("bases_on_balls"),
                era,
                whip,
                k_per_nine,
                bb_per_nine,
                hr_per_nine,
                k_per_bb,
                k_rate,
                bb_rate,
                k_minus_bb,
                hr_per_fb,
                func.sum(db.PitchStats.pitch_count).label("pitch_count"),
                func.sum(db.PitchStats.strikes).label("strikes"),
                func.sum(db.PitchStats.strikes_contact).label("strikes_contact"),
                func.sum(db.PitchStats.strikes_swinging).label("strikes_swinging"),
                func.sum(db.PitchStats.strikes_looking).label("strikes_looking"),
                func.sum(db.PitchStats.ground_balls).label("ground_balls"),
                func.sum(db.PitchStats.fly_balls).label("fly_balls"),
                func.sum(db.PitchStats.line_drives).label("line_drives"),
                func.sum(db.PitchStats.unknown_type).label("unknown_type"),
                func.sum(db.PitchStats.inherited_runners).label("inherited_runners"),
                func.sum(db.PitchStats.inherited_scored).label("inherited_scored"),
                func.sum(db.PitchStats.wpa_pitch).label("wpa_pitch"),
                func.sum(db.PitchStats.re24_pitch).label("re24_pitch"),
            ]
        )
        .select_from(join(db.PitchStats, db.Season, db.PitchStats.season_id == db.Season.id))
        .group_by(db.PitchStats.season_id)
        .group_by(db.PitchStats.player_id)
        .order_by(db.PitchStats.player_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pitch_stats_by_year_for_player(cls, db_engine, mlb_id):
        s = select([cls]).where(cls.mlb_id == mlb_id)
        results = db_engine.execute(s).fetchall()
        return PitchStatsMetrics.from_query_results(results) if results else []


class PitchStats_By_Team_View(db.Base):
    __table__ = create_view(
        name="pitchstats_by_team",
        selectable=select(
            [
                db.PitchStats.player_id.label("id"),
                db.PitchStats.player_id_mlb.label("mlb_id"),
                db.PitchStats.player_id_bbref.label("bbref_id"),
                db.PitchStats.player_team_id_bbref.label("team_id_bbref"),
                func.count(db.PitchStats.id).label("total_games"),
                func.sum(db.PitchStats.is_sp).label("games_as_sp"),
                func.sum(db.PitchStats.is_rp).label("games_as_rp"),
                func.sum(db.PitchStats.is_wp).label("wins"),
                func.sum(db.PitchStats.is_lp).label("losses"),
                func.sum(db.PitchStats.is_sv).label("saves"),
                innings_pitched,
                func.sum(db.PitchStats.total_outs).label("total_outs"),
                func.sum(db.PitchStats.batters_faced).label("batters_faced"),
                func.sum(db.PitchStats.runs).label("runs"),
                func.sum(db.PitchStats.earned_runs).label("earned_runs"),
                func.sum(db.PitchStats.hits).label("hits"),
                func.sum(db.PitchStats.homeruns).label("homeruns"),
                func.sum(db.PitchStats.strikeouts).label("strikeouts"),
                func.sum(db.PitchStats.bases_on_balls).label("bases_on_balls"),
                era,
                whip,
                k_per_nine,
                bb_per_nine,
                hr_per_nine,
                k_per_bb,
                k_rate,
                bb_rate,
                k_minus_bb,
                hr_per_fb,
                func.sum(db.PitchStats.pitch_count).label("pitch_count"),
                func.sum(db.PitchStats.strikes).label("strikes"),
                func.sum(db.PitchStats.strikes_contact).label("strikes_contact"),
                func.sum(db.PitchStats.strikes_swinging).label("strikes_swinging"),
                func.sum(db.PitchStats.strikes_looking).label("strikes_looking"),
                func.sum(db.PitchStats.ground_balls).label("ground_balls"),
                func.sum(db.PitchStats.fly_balls).label("fly_balls"),
                func.sum(db.PitchStats.line_drives).label("line_drives"),
                func.sum(db.PitchStats.unknown_type).label("unknown_type"),
                func.sum(db.PitchStats.inherited_runners).label("inherited_runners"),
                func.sum(db.PitchStats.inherited_scored).label("inherited_scored"),
                func.sum(db.PitchStats.wpa_pitch).label("wpa_pitch"),
                func.sum(db.PitchStats.re24_pitch).label("re24_pitch"),
            ]
        )
        .select_from(db.PitchStats)
        .group_by(db.PitchStats.player_id)
        .group_by(db.PitchStats.player_team_id_bbref)
        .order_by(db.PitchStats.player_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pitch_stats_by_team_for_player(cls, db_engine, mlb_id):
        s = select([cls]).where(cls.mlb_id == mlb_id)
        results = db_engine.execute(s).fetchall()
        return PitchStatsMetrics.from_query_results(results) if results else []


class PitchStats_By_Team_By_Year_View(db.Base):
    __table__ = create_view(
        name="pitchstats_by_team_by_year",
        selectable=select(
            [
                db.PitchStats.player_id.label("id"),
                db.PitchStats.player_id_mlb.label("mlb_id"),
                db.PitchStats.player_id_bbref.label("bbref_id"),
                db.PitchStats.season_id.label("season_id"),
                db.Season.year.label("year"),
                db.PitchStats.player_team_id.label("player_team_id"),
                db.PitchStats.player_team_id_bbref.label("team_id_bbref"),
                db.Assoc_Player_Team.stint_number.label("stint_number"),
                func.count(db.PitchStats.id).label("total_games"),
                func.sum(db.PitchStats.is_sp).label("games_as_sp"),
                func.sum(db.PitchStats.is_rp).label("games_as_rp"),
                func.sum(db.PitchStats.is_wp).label("wins"),
                func.sum(db.PitchStats.is_lp).label("losses"),
                func.sum(db.PitchStats.is_sv).label("saves"),
                innings_pitched,
                func.sum(db.PitchStats.total_outs).label("total_outs"),
                func.sum(db.PitchStats.batters_faced).label("batters_faced"),
                func.sum(db.PitchStats.runs).label("runs"),
                func.sum(db.PitchStats.earned_runs).label("earned_runs"),
                func.sum(db.PitchStats.hits).label("hits"),
                func.sum(db.PitchStats.homeruns).label("homeruns"),
                func.sum(db.PitchStats.strikeouts).label("strikeouts"),
                func.sum(db.PitchStats.bases_on_balls).label("bases_on_balls"),
                era,
                whip,
                k_per_nine,
                bb_per_nine,
                hr_per_nine,
                k_per_bb,
                k_rate,
                bb_rate,
                k_minus_bb,
                hr_per_fb,
                func.sum(db.PitchStats.pitch_count).label("pitch_count"),
                func.sum(db.PitchStats.strikes).label("strikes"),
                func.sum(db.PitchStats.strikes_contact).label("strikes_contact"),
                func.sum(db.PitchStats.strikes_swinging).label("strikes_swinging"),
                func.sum(db.PitchStats.strikes_looking).label("strikes_looking"),
                func.sum(db.PitchStats.ground_balls).label("ground_balls"),
                func.sum(db.PitchStats.fly_balls).label("fly_balls"),
                func.sum(db.PitchStats.line_drives).label("line_drives"),
                func.sum(db.PitchStats.unknown_type).label("unknown_type"),
                func.sum(db.PitchStats.inherited_runners).label("inherited_runners"),
                func.sum(db.PitchStats.inherited_scored).label("inherited_scored"),
                func.sum(db.PitchStats.wpa_pitch).label("wpa_pitch"),
                func.sum(db.PitchStats.re24_pitch).label("re24_pitch"),
            ]
        )
        .select_from(
            join(
                join(db.PitchStats, db.Season, db.PitchStats.season_id == db.Season.id),
                db.Assoc_Player_Team,
                and_(
                    db.PitchStats.player_id == db.Assoc_Player_Team.db_player_id,
                    db.PitchStats.player_team_id == db.Assoc_Player_Team.db_team_id,
                ),
            )
        )
        .group_by(db.PitchStats.season_id)
        .group_by(db.PitchStats.player_id)
        .group_by(db.PitchStats.player_team_id)
        .order_by(db.PitchStats.player_id_mlb)
        .order_by(db.PitchStats.season_id)
        .order_by(db.Assoc_Player_Team.stint_number),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pitch_stats_by_team_by_year_for_player(cls, db_engine, mlb_id):
        s = select([cls]).where(cls.mlb_id == mlb_id)
        results = db_engine.execute(s).fetchall()
        return PitchStatsMetrics.from_query_results(results) if results else []


class PitchStats_By_Opp_Team_View(db.Base):
    __table__ = create_view(
        name="pitchstats_by_opp_team",
        selectable=select(
            [
                db.PitchStats.player_id.label("id"),
                db.PitchStats.player_id_mlb.label("mlb_id"),
                db.PitchStats.player_id_bbref.label("bbref_id"),
                db.PitchStats.opponent_team_id_bbref.label("opponent_team_id_bbref"),
                func.count(db.PitchStats.id).label("total_games"),
                func.sum(db.PitchStats.is_sp).label("games_as_sp"),
                func.sum(db.PitchStats.is_rp).label("games_as_rp"),
                func.sum(db.PitchStats.is_wp).label("wins"),
                func.sum(db.PitchStats.is_lp).label("losses"),
                func.sum(db.PitchStats.is_sv).label("saves"),
                innings_pitched,
                func.sum(db.PitchStats.total_outs).label("total_outs"),
                func.sum(db.PitchStats.batters_faced).label("batters_faced"),
                func.sum(db.PitchStats.runs).label("runs"),
                func.sum(db.PitchStats.earned_runs).label("earned_runs"),
                func.sum(db.PitchStats.hits).label("hits"),
                func.sum(db.PitchStats.homeruns).label("homeruns"),
                func.sum(db.PitchStats.strikeouts).label("strikeouts"),
                func.sum(db.PitchStats.bases_on_balls).label("bases_on_balls"),
                era,
                whip,
                k_per_nine,
                bb_per_nine,
                hr_per_nine,
                k_per_bb,
                k_rate,
                bb_rate,
                k_minus_bb,
                hr_per_fb,
                func.sum(db.PitchStats.pitch_count).label("pitch_count"),
                func.sum(db.PitchStats.strikes).label("strikes"),
                func.sum(db.PitchStats.strikes_contact).label("strikes_contact"),
                func.sum(db.PitchStats.strikes_swinging).label("strikes_swinging"),
                func.sum(db.PitchStats.strikes_looking).label("strikes_looking"),
                func.sum(db.PitchStats.ground_balls).label("ground_balls"),
                func.sum(db.PitchStats.fly_balls).label("fly_balls"),
                func.sum(db.PitchStats.line_drives).label("line_drives"),
                func.sum(db.PitchStats.unknown_type).label("unknown_type"),
                func.sum(db.PitchStats.inherited_runners).label("inherited_runners"),
                func.sum(db.PitchStats.inherited_scored).label("inherited_scored"),
                func.sum(db.PitchStats.wpa_pitch).label("wpa_pitch"),
                func.sum(db.PitchStats.re24_pitch).label("re24_pitch"),
            ]
        )
        .select_from(db.PitchStats)
        .group_by(db.PitchStats.player_id)
        .group_by(db.PitchStats.opponent_team_id_bbref)
        .order_by(db.PitchStats.player_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pitch_stats_by_opp_team_for_player(cls, db_engine, mlb_id):
        s = select([cls]).where(cls.mlb_id == mlb_id)
        results = db_engine.execute(s).fetchall()
        return PitchStatsMetrics.from_query_results(results) if results else []


class PitchStats_By_Opp_Team_By_Year_View(db.Base):
    __table__ = create_view(
        name="pitchstats_by_opp_team_by_year",
        selectable=select(
            [
                db.PitchStats.player_id.label("id"),
                db.PitchStats.player_id_mlb.label("mlb_id"),
                db.PitchStats.player_id_bbref.label("bbref_id"),
                db.PitchStats.season_id.label("season_id"),
                db.Season.year.label("year"),
                db.PitchStats.opponent_team_id.label("opponent_team_id"),
                db.PitchStats.opponent_team_id_bbref.label("opponent_team_id_bbref"),
                func.count(db.PitchStats.id).label("total_games"),
                func.sum(db.PitchStats.is_sp).label("games_as_sp"),
                func.sum(db.PitchStats.is_rp).label("games_as_rp"),
                func.sum(db.PitchStats.is_wp).label("wins"),
                func.sum(db.PitchStats.is_lp).label("losses"),
                func.sum(db.PitchStats.is_sv).label("saves"),
                innings_pitched,
                func.sum(db.PitchStats.total_outs).label("total_outs"),
                func.sum(db.PitchStats.batters_faced).label("batters_faced"),
                func.sum(db.PitchStats.runs).label("runs"),
                func.sum(db.PitchStats.earned_runs).label("earned_runs"),
                func.sum(db.PitchStats.hits).label("hits"),
                func.sum(db.PitchStats.homeruns).label("homeruns"),
                func.sum(db.PitchStats.strikeouts).label("strikeouts"),
                func.sum(db.PitchStats.bases_on_balls).label("bases_on_balls"),
                era,
                whip,
                k_per_nine,
                bb_per_nine,
                hr_per_nine,
                k_per_bb,
                k_rate,
                bb_rate,
                k_minus_bb,
                hr_per_fb,
                func.sum(db.PitchStats.pitch_count).label("pitch_count"),
                func.sum(db.PitchStats.strikes).label("strikes"),
                func.sum(db.PitchStats.strikes_contact).label("strikes_contact"),
                func.sum(db.PitchStats.strikes_swinging).label("strikes_swinging"),
                func.sum(db.PitchStats.strikes_looking).label("strikes_looking"),
                func.sum(db.PitchStats.ground_balls).label("ground_balls"),
                func.sum(db.PitchStats.fly_balls).label("fly_balls"),
                func.sum(db.PitchStats.line_drives).label("line_drives"),
                func.sum(db.PitchStats.unknown_type).label("unknown_type"),
                func.sum(db.PitchStats.inherited_runners).label("inherited_runners"),
                func.sum(db.PitchStats.inherited_scored).label("inherited_scored"),
                func.sum(db.PitchStats.wpa_pitch).label("wpa_pitch"),
                func.sum(db.PitchStats.re24_pitch).label("re24_pitch"),
            ]
        )
        .select_from(join(db.PitchStats, db.Season, db.PitchStats.season_id == db.Season.id))
        .group_by(db.PitchStats.season_id)
        .group_by(db.PitchStats.player_id)
        .group_by(db.PitchStats.opponent_team_id)
        .order_by(db.PitchStats.player_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pitch_stats_by_opp_team_by_year_for_player(cls, db_engine, mlb_id):
        s = select([cls]).where(cls.mlb_id == mlb_id)
        results = db_engine.execute(s).fetchall()
        return PitchStatsMetrics.from_query_results(results) if results else []
