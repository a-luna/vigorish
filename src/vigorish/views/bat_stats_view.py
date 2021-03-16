from sqlalchemy import and_, func, join, select
from sqlalchemy_utils.view import create_view

import vigorish.database as db
from vigorish.data.metrics.bat_stats import BatStatsMetrics
from vigorish.views.bat_stats_col_expressions import (
    avg,
    bb_rate,
    contact_rate,
    iso,
    k_rate,
    obp,
    ops,
    slg,
)


class BatStats_All_View(db.Base):
    __table__ = create_view(
        name="batstats_all",
        selectable=select(
            [
                db.BatStats.player_id.label("id"),
                db.BatStats.player_id_mlb.label("mlb_id"),
                db.BatStats.player_id_bbref.label("bbref_id"),
                func.count(db.BatStats.id).label("total_games"),
                avg,
                obp,
                slg,
                ops,
                iso,
                bb_rate,
                k_rate,
                contact_rate,
                func.sum(db.BatStats.plate_appearances).label("plate_appearances"),
                func.sum(db.BatStats.at_bats).label("at_bats"),
                func.sum(db.BatStats.hits).label("hits"),
                func.sum(db.BatStats.runs_scored).label("runs_scored"),
                func.sum(db.BatStats.rbis).label("rbis"),
                func.sum(db.BatStats.bases_on_balls).label("bases_on_balls"),
                func.sum(db.BatStats.strikeouts).label("strikeouts"),
                func.sum(db.BatStats.doubles).label("doubles"),
                func.sum(db.BatStats.triples).label("triples"),
                func.sum(db.BatStats.homeruns).label("homeruns"),
                func.sum(db.BatStats.stolen_bases).label("stolen_bases"),
                func.sum(db.BatStats.caught_stealing).label("caught_stealing"),
                func.sum(db.BatStats.hit_by_pitch).label("hit_by_pitch"),
                func.sum(db.BatStats.intentional_bb).label("intentional_bb"),
                func.sum(db.BatStats.gdp).label("gdp"),
                func.sum(db.BatStats.sac_fly).label("sac_fly"),
                func.sum(db.BatStats.sac_hit).label("sac_hit"),
                func.sum(db.BatStats.total_pitches).label("total_pitches"),
                func.sum(db.BatStats.total_strikes).label("total_strikes"),
                func.sum(db.BatStats.wpa_bat).label("wpa_bat"),
                func.sum(db.BatStats.wpa_bat_pos).label("wpa_bat_pos"),
                func.sum(db.BatStats.wpa_bat_neg).label("wpa_bat_neg"),
                func.sum(db.BatStats.re24_bat).label("re24_bat"),
            ]
        )
        .select_from(db.BatStats)
        .group_by(db.BatStats.player_id)
        .order_by(db.BatStats.player_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_bat_stats_for_career_for_player(cls, db_engine, mlb_id):
        s = select([cls]).where(cls.mlb_id == mlb_id)
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results)[0] if results else None


class BatStats_By_Year_View(db.Base):
    __table__ = create_view(
        name="batstats_by_year",
        selectable=select(
            [
                db.BatStats.player_id.label("id"),
                db.BatStats.player_id_mlb.label("mlb_id"),
                db.BatStats.player_id_bbref.label("bbref_id"),
                db.BatStats.season_id.label("season_id"),
                db.Season.year.label("year"),
                func.count(db.BatStats.id).label("total_games"),
                avg,
                obp,
                slg,
                ops,
                iso,
                bb_rate,
                k_rate,
                contact_rate,
                func.sum(db.BatStats.plate_appearances).label("plate_appearances"),
                func.sum(db.BatStats.at_bats).label("at_bats"),
                func.sum(db.BatStats.hits).label("hits"),
                func.sum(db.BatStats.runs_scored).label("runs_scored"),
                func.sum(db.BatStats.rbis).label("rbis"),
                func.sum(db.BatStats.bases_on_balls).label("bases_on_balls"),
                func.sum(db.BatStats.strikeouts).label("strikeouts"),
                func.sum(db.BatStats.doubles).label("doubles"),
                func.sum(db.BatStats.triples).label("triples"),
                func.sum(db.BatStats.homeruns).label("homeruns"),
                func.sum(db.BatStats.stolen_bases).label("stolen_bases"),
                func.sum(db.BatStats.caught_stealing).label("caught_stealing"),
                func.sum(db.BatStats.hit_by_pitch).label("hit_by_pitch"),
                func.sum(db.BatStats.intentional_bb).label("intentional_bb"),
                func.sum(db.BatStats.gdp).label("gdp"),
                func.sum(db.BatStats.sac_fly).label("sac_fly"),
                func.sum(db.BatStats.sac_hit).label("sac_hit"),
                func.sum(db.BatStats.total_pitches).label("total_pitches"),
                func.sum(db.BatStats.total_strikes).label("total_strikes"),
                func.sum(db.BatStats.wpa_bat).label("wpa_bat"),
                func.sum(db.BatStats.wpa_bat_pos).label("wpa_bat_pos"),
                func.sum(db.BatStats.wpa_bat_neg).label("wpa_bat_neg"),
                func.sum(db.BatStats.re24_bat).label("re24_bat"),
            ]
        )
        .select_from(join(db.BatStats, db.Season, db.BatStats.season_id == db.Season.id))
        .group_by(db.BatStats.season_id)
        .group_by(db.BatStats.player_id)
        .order_by(db.BatStats.player_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_bat_stats_by_year_for_player(cls, db_engine, mlb_id):
        s = select([cls]).where(cls.mlb_id == mlb_id)
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results) if results else []


class BatStats_By_Team_View(db.Base):
    __table__ = create_view(
        name="batstats_by_team",
        selectable=select(
            [
                db.BatStats.player_id.label("id"),
                db.BatStats.player_id_mlb.label("mlb_id"),
                db.BatStats.player_id_bbref.label("bbref_id"),
                db.BatStats.player_team_id_bbref.label("player_team_id_bbref"),
                func.count(db.BatStats.id).label("total_games"),
                avg,
                obp,
                slg,
                ops,
                iso,
                bb_rate,
                k_rate,
                contact_rate,
                func.sum(db.BatStats.plate_appearances).label("plate_appearances"),
                func.sum(db.BatStats.at_bats).label("at_bats"),
                func.sum(db.BatStats.hits).label("hits"),
                func.sum(db.BatStats.runs_scored).label("runs_scored"),
                func.sum(db.BatStats.rbis).label("rbis"),
                func.sum(db.BatStats.bases_on_balls).label("bases_on_balls"),
                func.sum(db.BatStats.strikeouts).label("strikeouts"),
                func.sum(db.BatStats.doubles).label("doubles"),
                func.sum(db.BatStats.triples).label("triples"),
                func.sum(db.BatStats.homeruns).label("homeruns"),
                func.sum(db.BatStats.stolen_bases).label("stolen_bases"),
                func.sum(db.BatStats.caught_stealing).label("caught_stealing"),
                func.sum(db.BatStats.hit_by_pitch).label("hit_by_pitch"),
                func.sum(db.BatStats.intentional_bb).label("intentional_bb"),
                func.sum(db.BatStats.gdp).label("gdp"),
                func.sum(db.BatStats.sac_fly).label("sac_fly"),
                func.sum(db.BatStats.sac_hit).label("sac_hit"),
                func.sum(db.BatStats.total_pitches).label("total_pitches"),
                func.sum(db.BatStats.total_strikes).label("total_strikes"),
                func.sum(db.BatStats.wpa_bat).label("wpa_bat"),
                func.sum(db.BatStats.wpa_bat_pos).label("wpa_bat_pos"),
                func.sum(db.BatStats.wpa_bat_neg).label("wpa_bat_neg"),
                func.sum(db.BatStats.re24_bat).label("re24_bat"),
            ]
        )
        .select_from(db.BatStats)
        .group_by(db.BatStats.player_id)
        .group_by(db.BatStats.player_team_id_bbref)
        .order_by(db.BatStats.player_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_bat_stats_by_team_for_player(cls, db_engine, mlb_id):
        s = select([cls]).where(cls.mlb_id == mlb_id)
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results) if results else []


class BatStats_By_Team_Year_View(db.Base):
    __table__ = create_view(
        name="batstats_by_team_year",
        selectable=select(
            [
                db.BatStats.player_id.label("id"),
                db.BatStats.player_id_mlb.label("mlb_id"),
                db.BatStats.player_id_bbref.label("bbref_id"),
                db.BatStats.season_id.label("season_id"),
                db.Season.year.label("year"),
                db.BatStats.player_team_id.label("player_team_id"),
                db.BatStats.player_team_id_bbref.label("player_team_id_bbref"),
                db.Assoc_Player_Team.stint_number.label("stint_number"),
                func.count(db.BatStats.id).label("total_games"),
                avg,
                obp,
                slg,
                ops,
                iso,
                bb_rate,
                k_rate,
                contact_rate,
                func.sum(db.BatStats.plate_appearances).label("plate_appearances"),
                func.sum(db.BatStats.at_bats).label("at_bats"),
                func.sum(db.BatStats.hits).label("hits"),
                func.sum(db.BatStats.runs_scored).label("runs_scored"),
                func.sum(db.BatStats.rbis).label("rbis"),
                func.sum(db.BatStats.bases_on_balls).label("bases_on_balls"),
                func.sum(db.BatStats.strikeouts).label("strikeouts"),
                func.sum(db.BatStats.doubles).label("doubles"),
                func.sum(db.BatStats.triples).label("triples"),
                func.sum(db.BatStats.homeruns).label("homeruns"),
                func.sum(db.BatStats.stolen_bases).label("stolen_bases"),
                func.sum(db.BatStats.caught_stealing).label("caught_stealing"),
                func.sum(db.BatStats.hit_by_pitch).label("hit_by_pitch"),
                func.sum(db.BatStats.intentional_bb).label("intentional_bb"),
                func.sum(db.BatStats.gdp).label("gdp"),
                func.sum(db.BatStats.sac_fly).label("sac_fly"),
                func.sum(db.BatStats.sac_hit).label("sac_hit"),
                func.sum(db.BatStats.total_pitches).label("total_pitches"),
                func.sum(db.BatStats.total_strikes).label("total_strikes"),
                func.sum(db.BatStats.wpa_bat).label("wpa_bat"),
                func.sum(db.BatStats.wpa_bat_pos).label("wpa_bat_pos"),
                func.sum(db.BatStats.wpa_bat_neg).label("wpa_bat_neg"),
                func.sum(db.BatStats.re24_bat).label("re24_bat"),
            ]
        )
        .select_from(
            join(
                join(db.BatStats, db.Season, db.BatStats.season_id == db.Season.id),
                db.Assoc_Player_Team,
                and_(
                    db.BatStats.player_id == db.Assoc_Player_Team.db_player_id,
                    db.BatStats.player_team_id == db.Assoc_Player_Team.db_team_id,
                ),
            )
        )
        .group_by(db.BatStats.season_id)
        .group_by(db.BatStats.player_id)
        .group_by(db.BatStats.player_team_id)
        .order_by(db.BatStats.player_id_mlb)
        .order_by(db.BatStats.season_id)
        .order_by(db.Assoc_Player_Team.stint_number),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_bat_stats_by_team_by_year_for_player(cls, db_engine, mlb_id):
        s = select([cls]).where(cls.mlb_id == mlb_id)
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results) if results else []


class BatStats_By_Opp_Team_View(db.Base):
    __table__ = create_view(
        name="batstats_by_opp_team",
        selectable=select(
            [
                db.BatStats.player_id.label("id"),
                db.BatStats.player_id_mlb.label("mlb_id"),
                db.BatStats.player_id_bbref.label("bbref_id"),
                db.BatStats.opponent_team_id_bbref.label("opponent_team_id_bbref"),
                func.count(db.BatStats.id).label("total_games"),
                avg,
                obp,
                slg,
                ops,
                iso,
                bb_rate,
                k_rate,
                contact_rate,
                func.sum(db.BatStats.plate_appearances).label("plate_appearances"),
                func.sum(db.BatStats.at_bats).label("at_bats"),
                func.sum(db.BatStats.hits).label("hits"),
                func.sum(db.BatStats.runs_scored).label("runs_scored"),
                func.sum(db.BatStats.rbis).label("rbis"),
                func.sum(db.BatStats.bases_on_balls).label("bases_on_balls"),
                func.sum(db.BatStats.strikeouts).label("strikeouts"),
                func.sum(db.BatStats.doubles).label("doubles"),
                func.sum(db.BatStats.triples).label("triples"),
                func.sum(db.BatStats.homeruns).label("homeruns"),
                func.sum(db.BatStats.stolen_bases).label("stolen_bases"),
                func.sum(db.BatStats.caught_stealing).label("caught_stealing"),
                func.sum(db.BatStats.hit_by_pitch).label("hit_by_pitch"),
                func.sum(db.BatStats.intentional_bb).label("intentional_bb"),
                func.sum(db.BatStats.gdp).label("gdp"),
                func.sum(db.BatStats.sac_fly).label("sac_fly"),
                func.sum(db.BatStats.sac_hit).label("sac_hit"),
                func.sum(db.BatStats.total_pitches).label("total_pitches"),
                func.sum(db.BatStats.total_strikes).label("total_strikes"),
                func.sum(db.BatStats.wpa_bat).label("wpa_bat"),
                func.sum(db.BatStats.wpa_bat_pos).label("wpa_bat_pos"),
                func.sum(db.BatStats.wpa_bat_neg).label("wpa_bat_neg"),
                func.sum(db.BatStats.re24_bat).label("re24_bat"),
            ]
        )
        .select_from(db.BatStats)
        .group_by(db.BatStats.player_id)
        .group_by(db.BatStats.opponent_team_id_bbref)
        .order_by(db.BatStats.player_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_bat_stats_by_opp_for_player(cls, db_engine, mlb_id):
        s = select([cls]).where(cls.mlb_id == mlb_id)
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results) if results else []


class BatStats_By_Opp_Team_Year_View(db.Base):
    __table__ = create_view(
        name="batstats_by_opp_team_year",
        selectable=select(
            [
                db.BatStats.player_id.label("id"),
                db.BatStats.player_id_mlb.label("mlb_id"),
                db.BatStats.player_id_bbref.label("bbref_id"),
                db.BatStats.season_id.label("season_id"),
                db.Season.year.label("year"),
                db.BatStats.opponent_team_id.label("opponent_team_id"),
                db.BatStats.opponent_team_id_bbref.label("opponent_team_id_bbref"),
                func.count(db.BatStats.id).label("total_games"),
                avg,
                obp,
                slg,
                ops,
                iso,
                bb_rate,
                k_rate,
                contact_rate,
                func.sum(db.BatStats.plate_appearances).label("plate_appearances"),
                func.sum(db.BatStats.at_bats).label("at_bats"),
                func.sum(db.BatStats.hits).label("hits"),
                func.sum(db.BatStats.runs_scored).label("runs_scored"),
                func.sum(db.BatStats.rbis).label("rbis"),
                func.sum(db.BatStats.bases_on_balls).label("bases_on_balls"),
                func.sum(db.BatStats.strikeouts).label("strikeouts"),
                func.sum(db.BatStats.doubles).label("doubles"),
                func.sum(db.BatStats.triples).label("triples"),
                func.sum(db.BatStats.homeruns).label("homeruns"),
                func.sum(db.BatStats.stolen_bases).label("stolen_bases"),
                func.sum(db.BatStats.caught_stealing).label("caught_stealing"),
                func.sum(db.BatStats.hit_by_pitch).label("hit_by_pitch"),
                func.sum(db.BatStats.intentional_bb).label("intentional_bb"),
                func.sum(db.BatStats.gdp).label("gdp"),
                func.sum(db.BatStats.sac_fly).label("sac_fly"),
                func.sum(db.BatStats.sac_hit).label("sac_hit"),
                func.sum(db.BatStats.total_pitches).label("total_pitches"),
                func.sum(db.BatStats.total_strikes).label("total_strikes"),
                func.sum(db.BatStats.wpa_bat).label("wpa_bat"),
                func.sum(db.BatStats.wpa_bat_pos).label("wpa_bat_pos"),
                func.sum(db.BatStats.wpa_bat_neg).label("wpa_bat_neg"),
                func.sum(db.BatStats.re24_bat).label("re24_bat"),
            ]
        )
        .select_from(join(db.BatStats, db.Season, db.BatStats.season_id == db.Season.id))
        .group_by(db.BatStats.season_id)
        .group_by(db.BatStats.player_id)
        .group_by(db.BatStats.opponent_team_id)
        .order_by(db.BatStats.player_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_bat_stats_by_opp_by_year_for_player(cls, db_engine, mlb_id):
        s = select([cls]).where(cls.mlb_id == mlb_id)
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results) if results else []
