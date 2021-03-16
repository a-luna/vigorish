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


class Team_BatStats_By_Year_View(db.Base):
    __table__ = create_view(
        name="team_batstats_by_year",
        selectable=select(
            [
                db.BatStats.player_team_id.label("team_id"),
                db.BatStats.player_team_id_bbref.label("team_id_bbref"),
                db.Season.year.label("year"),
                func.count(db.BatStats.game_status_id.distinct()).label("total_games"),
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
                db.BatStats.season_id.label("season_id"),
            ]
        )
        .select_from(join(db.BatStats, db.Season, db.BatStats.season_id == db.Season.id))
        .group_by(db.BatStats.season_id)
        .group_by(db.BatStats.player_team_id)
        .order_by(db.BatStats.player_team_id)
        .order_by(db.BatStats.season_id),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_bat_stats_for_team(cls, db_engine, team_id_bbref, year):
        s = select([cls]).where(and_(cls.team_id_bbref == team_id_bbref, cls.year == year))
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results)[0] if results else None

    @classmethod
    def get_bat_stats_by_year_for_team(cls, db_engine, team_id_bbref):
        s = select([cls]).where(cls.team_id_bbref == team_id_bbref)
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results) if results else []

    @classmethod
    def get_bat_stats_for_season_for_all_teams(cls, db_engine, year):
        s = select([cls]).where(cls.year == year)
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results) if results else []


class Team_BatStats_By_BatOrder_By_Year(db.Base):
    __table__ = create_view(
        name="team_batstats_by_batorder_by_year",
        selectable=select(
            [
                db.BatStats.player_team_id.label("team_id"),
                db.BatStats.player_team_id_bbref.label("team_id_bbref"),
                db.Season.year.label("year"),
                db.BatStats.bat_order.label("bat_order"),
                func.count(db.BatStats.game_status_id.distinct()).label("total_games"),
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
                db.BatStats.season_id.label("season_id"),
            ]
        )
        .select_from(join(db.BatStats, db.Season, db.BatStats.season_id == db.Season.id))
        .group_by(db.BatStats.season_id)
        .group_by(db.BatStats.player_team_id)
        .group_by(db.BatStats.bat_order)
        .order_by(db.BatStats.bat_order),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_bat_stats_by_lineup_spot_for_team(cls, db_engine, team_id_bbref, year):
        s = select([cls]).where(and_(cls.team_id_bbref == team_id_bbref, cls.year == year))
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results) if results else []

    @classmethod
    def get_bat_stats_for_lineup_spot_by_year_for_team(cls, db_engine, lineup_spot, team_id_bbref):
        s = select([cls]).where(and_(cls.bat_order == lineup_spot, cls.team_id_bbref == team_id_bbref))
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results) if results else []

    @classmethod
    def get_bat_stats_for_lineup_spot_for_season_for_all_teams(cls, db_engine, lineup_spot, year):
        s = select([cls]).where(and_(cls.bat_order == lineup_spot, cls.year == year))
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results) if results else []


class Team_BatStats_By_DefPosition_By_Year(db.Base):
    __table__ = create_view(
        name="team_batstats_by_defpos_by_year",
        selectable=select(
            [
                db.BatStats.player_team_id.label("team_id"),
                db.BatStats.player_team_id_bbref.label("team_id_bbref"),
                db.Season.year.label("year"),
                db.BatStats.def_position.label("def_position"),
                func.count(db.BatStats.game_status_id.distinct()).label("total_games"),
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
                db.BatStats.season_id.label("season_id"),
            ]
        )
        .select_from(join(db.BatStats, db.Season, db.BatStats.season_id == db.Season.id))
        .group_by(db.BatStats.season_id)
        .group_by(db.BatStats.player_team_id)
        .group_by(db.BatStats.def_position)
        .order_by(db.BatStats.def_position),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_bat_stats_by_defpos_for_team(cls, db_engine, team_id_bbref, year):
        s = select([cls]).where(and_(cls.team_id_bbref == team_id_bbref, cls.year == year))
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results) if results else []

    @classmethod
    def get_bat_stats_for_defpos_by_year_for_team(cls, db_engine, def_position, team_id_bbref):
        s = select([cls]).where(and_(cls.def_position == def_position, cls.team_id_bbref == team_id_bbref))
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results) if results else []

    @classmethod
    def get_bat_stats_for_defpos_for_season_for_all_teams(cls, db_engine, def_position, year):
        s = select([cls]).where(and_(cls.def_position == def_position, cls.year == year))
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results) if results else []


class Team_BatStats_For_Starters_By_Year(db.Base):
    __table__ = create_view(
        name="team_batstats_for_starters_by_year",
        selectable=select(
            [
                db.BatStats.player_team_id.label("team_id"),
                db.BatStats.player_team_id_bbref.label("team_id_bbref"),
                db.Season.year.label("year"),
                db.BatStats.is_starter.label("is_starter"),
                func.count(db.BatStats.game_status_id.distinct()).label("total_games"),
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
                db.BatStats.season_id.label("season_id"),
            ]
        )
        .select_from(join(db.BatStats, db.Season, db.BatStats.season_id == db.Season.id))
        .where(db.BatStats.is_starter == 1)
        .group_by(db.BatStats.season_id)
        .group_by(db.BatStats.player_team_id)
        .order_by(db.BatStats.player_team_id),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_bat_stats_for_starters_for_team(cls, db_engine, team_id_bbref, year):
        s = select([cls]).where(and_(cls.team_id_bbref == team_id_bbref, cls.year == year))
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results)[0] if results else None

    @classmethod
    def get_bat_stats_for_starters_by_year_for_team(cls, db_engine, team_id_bbref):
        s = select([cls]).where(cls.team_id_bbref == team_id_bbref)
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results) if results else []

    @classmethod
    def get_bat_stats_for_starters_for_season_for_all_teams(cls, db_engine, year):
        s = select([cls]).where(cls.year == year)
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results) if results else []


class Team_BatStats_For_Subs_By_Year(db.Base):
    __table__ = create_view(
        name="team_batstats_for_subs_by_year",
        selectable=select(
            [
                db.BatStats.player_team_id.label("team_id"),
                db.BatStats.player_team_id_bbref.label("team_id_bbref"),
                db.Season.year.label("year"),
                db.BatStats.is_starter.label("is_starter"),
                func.count(db.BatStats.game_status_id.distinct()).label("total_games"),
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
                db.BatStats.season_id.label("season_id"),
            ]
        )
        .select_from(join(db.BatStats, db.Season, db.BatStats.season_id == db.Season.id))
        .where(db.BatStats.is_starter == 0)
        .group_by(db.BatStats.season_id)
        .group_by(db.BatStats.player_team_id)
        .order_by(db.BatStats.player_team_id),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_bat_stats_for_subs_for_team(cls, db_engine, team_id_bbref, year):
        s = select([cls]).where(and_(cls.team_id_bbref == team_id_bbref, cls.year == year))
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results)[0] if results else None

    @classmethod
    def get_bat_stats_for_subs_by_year_for_team(cls, db_engine, team_id_bbref):
        s = select([cls]).where(cls.team_id_bbref == team_id_bbref)
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results) if results else []

    @classmethod
    def get_bat_stats_for_subs_for_season_for_all_teams(cls, db_engine, year):
        s = select([cls]).where(cls.year == year)
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results) if results else []


class Team_BatStats_By_Player_By_Year_View(db.Base):
    __table__ = create_view(
        name="team_batstats_by_player_by_year",
        selectable=select(
            [
                db.BatStats.player_team_id.label("team_id"),
                db.BatStats.player_team_id_bbref.label("team_id_bbref"),
                db.Season.year.label("year"),
                db.BatStats.player_id_mlb.label("mlb_id"),
                db.BatStats.player_id_bbref.label("bbref_id"),
                func.count(db.BatStats.game_status_id.distinct()).label("total_games"),
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
                db.BatStats.player_id.label("player_id"),
                db.BatStats.season_id.label("season_id"),
            ]
        )
        .select_from(join(db.BatStats, db.Season, db.BatStats.season_id == db.Season.id))
        .group_by(db.BatStats.season_id)
        .group_by(db.BatStats.player_id)
        .group_by(db.BatStats.player_team_id)
        .order_by(db.BatStats.plate_appearances),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_bat_stats_by_player_for_team(cls, db_engine, team_id_bbref, year):
        s = select([cls]).where(and_(cls.team_id_bbref == team_id_bbref, cls.year == year))
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results) if results else []


class Team_BatStats_By_BatOrder_By_Player_By_Year(db.Base):
    __table__ = create_view(
        name="team_batstats_by_batorder_by_player_by_year",
        selectable=select(
            [
                db.BatStats.player_team_id.label("team_id"),
                db.BatStats.player_team_id_bbref.label("team_id_bbref"),
                db.Season.year.label("year"),
                db.BatStats.bat_order.label("bat_order"),
                db.BatStats.player_id_mlb.label("mlb_id"),
                db.BatStats.player_id_bbref.label("bbref_id"),
                func.count(db.BatStats.game_status_id.distinct()).label("total_games"),
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
                db.BatStats.player_id.label("player_id"),
                db.BatStats.season_id.label("season_id"),
            ]
        )
        .select_from(join(db.BatStats, db.Season, db.BatStats.season_id == db.Season.id))
        .group_by(db.BatStats.player_team_id)
        .group_by(db.BatStats.player_id)
        .group_by(db.BatStats.bat_order)
        .order_by(db.BatStats.bat_order),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_bat_stats_for_lineup_spot_by_player_for_team(cls, db_engine, lineup_spot, team_id_bbref, year):
        s = select([cls]).where(
            and_(cls.bat_order == lineup_spot, cls.team_id_bbref == team_id_bbref, cls.year == year)
        )
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results) if results else []


class Team_BatStats_By_DefPosition_By_Player_By_Year(db.Base):
    __table__ = create_view(
        name="team_batstats_by_defpos_by_player_by_year",
        selectable=select(
            [
                db.BatStats.player_team_id.label("team_id"),
                db.BatStats.player_team_id_bbref.label("team_id_bbref"),
                db.Season.year.label("year"),
                db.BatStats.def_position.label("def_position"),
                db.BatStats.player_id_mlb.label("mlb_id"),
                db.BatStats.player_id_bbref.label("bbref_id"),
                func.count(db.BatStats.game_status_id.distinct()).label("total_games"),
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
                db.BatStats.player_id.label("player_id"),
                db.BatStats.season_id.label("season_id"),
            ]
        )
        .select_from(join(db.BatStats, db.Season, db.BatStats.season_id == db.Season.id))
        .group_by(db.BatStats.player_team_id)
        .group_by(db.BatStats.player_id)
        .group_by(db.BatStats.def_position)
        .order_by(db.BatStats.def_position),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_bat_stats_for_defpos_by_player_for_team(cls, db_engine, def_position, team_id_bbref, year):
        s = select([cls]).where(
            and_(cls.def_position == def_position, cls.team_id_bbref == team_id_bbref, cls.year == year)
        )
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results) if results else []


class Team_BatStats_For_Starters_By_Player_By_Year(db.Base):
    __table__ = create_view(
        name="team_batstats_for_starters_by_player_by_year",
        selectable=select(
            [
                db.BatStats.player_team_id.label("team_id"),
                db.BatStats.player_team_id_bbref.label("team_id_bbref"),
                db.Season.year.label("year"),
                db.BatStats.is_starter.label("is_starter"),
                db.BatStats.player_id_mlb.label("mlb_id"),
                db.BatStats.player_id_bbref.label("bbref_id"),
                func.count(db.BatStats.game_status_id.distinct()).label("total_games"),
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
                db.BatStats.player_id.label("player_id"),
                db.BatStats.season_id.label("season_id"),
            ]
        )
        .select_from(join(db.BatStats, db.Season, db.BatStats.season_id == db.Season.id))
        .where(db.BatStats.is_starter == 1)
        .group_by(db.BatStats.player_team_id)
        .group_by(db.BatStats.player_id)
        .group_by(db.BatStats.def_position)
        .order_by(db.BatStats.def_position),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_bat_stats_for_starters_by_player_for_team(cls, db_engine, team_id_bbref, year):
        s = select([cls]).where(and_(cls.team_id_bbref == team_id_bbref, cls.year == year))
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results) if results else []


class Team_BatStats_For_Subs_By_Player_By_Year(db.Base):
    __table__ = create_view(
        name="team_batstats_for_subs_by_player_by_year",
        selectable=select(
            [
                db.BatStats.player_team_id.label("team_id"),
                db.BatStats.player_team_id_bbref.label("team_id_bbref"),
                db.Season.year.label("year"),
                db.BatStats.is_starter.label("is_starter"),
                db.BatStats.player_id_mlb.label("mlb_id"),
                db.BatStats.player_id_bbref.label("bbref_id"),
                func.count(db.BatStats.game_status_id.distinct()).label("total_games"),
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
                db.BatStats.player_id.label("player_id"),
                db.BatStats.season_id.label("season_id"),
            ]
        )
        .select_from(join(db.BatStats, db.Season, db.BatStats.season_id == db.Season.id))
        .where(db.BatStats.is_starter == 0)
        .group_by(db.BatStats.player_team_id)
        .group_by(db.BatStats.player_id)
        .group_by(db.BatStats.def_position)
        .order_by(db.BatStats.def_position),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_bat_stats_for_subs_by_player_for_team(cls, db_engine, team_id_bbref, year):
        s = select([cls]).where(and_(cls.team_id_bbref == team_id_bbref, cls.year == year))
        results = db_engine.execute(s).fetchall()
        return BatStatsMetrics.from_query_results(results) if results else []
