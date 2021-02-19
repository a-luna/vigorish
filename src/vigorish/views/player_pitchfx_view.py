from sqlalchemy import and_, func, or_, select
from sqlalchemy_utils import create_view

import vigorish.database as db
from vigorish.data.metrics import PitchFxMetrics, PitchFxMetricsCollection
from vigorish.enums import PitchType
from vigorish.views.pfx_col_expressions import (
    called_strike_rate,
    contact_rate,
    csw_rate,
    custom_score,
    fly_ball_rate,
    ground_ball_rate,
    line_drive_rate,
    money_pitch,
    o_contact_rate,
    o_swing_rate,
    pop_up_rate,
    swing_rate,
    swinging_strike_rate,
    whiff_rate,
    z_contact_rate,
    z_swing_rate,
    zone_rate,
)


class Pitch_Type_All_View(db.Base):
    __table__ = create_view(
        name="pitch_type_all",
        selectable=select(
            [
                db.PitchFx.pitcher_id.label("id"),
                db.PitchFx.pitcher_id_mlb.label("pitcher_id_mlb"),
                db.PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.sum(db.PitchFx.batter_did_swing).label("total_swings"),
                func.sum(db.PitchFx.batter_made_contact).label("total_swings_made_contact"),
                func.sum(db.PitchFx.called_strike).label("total_called_strikes"),
                func.sum(db.PitchFx.swinging_strike).label("total_swinging_strikes"),
                func.sum(db.PitchFx.inside_strike_zone).label("total_inside_strike_zone"),
                func.sum(db.PitchFx.outside_strike_zone).label("total_outside_strike_zone"),
                func.sum(db.PitchFx.swing_inside_zone).label("total_swings_inside_zone"),
                func.sum(db.PitchFx.swing_outside_zone).label("total_swings_outside_zone"),
                func.sum(db.PitchFx.contact_inside_zone).label("total_contact_inside_zone"),
                func.sum(db.PitchFx.contact_outside_zone).label("total_contact_outside_zone"),
                zone_rate,
                called_strike_rate,
                swinging_strike_rate,
                whiff_rate,
                csw_rate,
                o_swing_rate,
                z_swing_rate,
                swing_rate,
                o_contact_rate,
                z_contact_rate,
                contact_rate,
                custom_score,
                money_pitch,
                func.sum(db.PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(db.PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(db.PitchFx.is_pop_up).label("total_pop_ups"),
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
            ]
        )
        .where(
            and_(
                db.PitchFx.is_duplicate_guid == 0,
                db.PitchFx.is_duplicate_pitch_number == 0,
                db.PitchFx.is_invalid_ibb == 0,
                db.PitchFx.is_out_of_sequence == 0,
                or_(db.PitchFx.stand == "L", db.PitchFx.stand == "R"),
            )
        )
        .select_from(db.PitchFx)
        .group_by(db.PitchFx.pitcher_id)
        .group_by(db.PitchFx.mlbam_pitch_name)
        .order_by(db.PitchFx.pitcher_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pitchfx_metrics_for_career_for_player(cls, db_engine, player_id):
        s = select([cls]).where(cls.id == player_id)
        results = db_engine.execute(s).fetchall()
        return PitchFxMetricsCollection.from_pitchfx_view_results(results) if results else None


class Pitch_Type_Right_View(db.Base):
    __table__ = create_view(
        name="pitch_type_right",
        selectable=select(
            [
                db.PitchFx.pitcher_id.label("id"),
                db.PitchFx.pitcher_id_mlb.label("pitcher_id_mlb"),
                db.PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.sum(db.PitchFx.batter_did_swing).label("total_swings"),
                func.sum(db.PitchFx.batter_made_contact).label("total_swings_made_contact"),
                func.sum(db.PitchFx.called_strike).label("total_called_strikes"),
                func.sum(db.PitchFx.swinging_strike).label("total_swinging_strikes"),
                func.sum(db.PitchFx.inside_strike_zone).label("total_inside_strike_zone"),
                func.sum(db.PitchFx.outside_strike_zone).label("total_outside_strike_zone"),
                func.sum(db.PitchFx.swing_inside_zone).label("total_swings_inside_zone"),
                func.sum(db.PitchFx.swing_outside_zone).label("total_swings_outside_zone"),
                func.sum(db.PitchFx.contact_inside_zone).label("total_contact_inside_zone"),
                func.sum(db.PitchFx.contact_outside_zone).label("total_contact_outside_zone"),
                zone_rate,
                called_strike_rate,
                swinging_strike_rate,
                whiff_rate,
                csw_rate,
                o_swing_rate,
                z_swing_rate,
                swing_rate,
                o_contact_rate,
                z_contact_rate,
                contact_rate,
                custom_score,
                func.sum(db.PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(db.PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(db.PitchFx.is_pop_up).label("total_pop_ups"),
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
            ]
        )
        .where(
            and_(
                db.PitchFx.is_duplicate_guid == 0,
                db.PitchFx.is_duplicate_pitch_number == 0,
                db.PitchFx.is_invalid_ibb == 0,
                db.PitchFx.is_out_of_sequence == 0,
                db.PitchFx.stand == "R",
            )
        )
        .select_from(db.PitchFx)
        .group_by(db.PitchFx.pitcher_id)
        .group_by(db.PitchFx.mlbam_pitch_name)
        .order_by(db.PitchFx.pitcher_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pitchfx_metrics_vs_rhb_for_player(cls, db_engine, player_id):
        s = select([cls]).where(cls.id == player_id)
        results = db_engine.execute(s).fetchall()
        return PitchFxMetricsCollection.from_pitchfx_view_results(results) if results else None


class Pitch_Type_Left_View(db.Base):
    __table__ = create_view(
        name="pitch_type_left",
        selectable=select(
            [
                db.PitchFx.pitcher_id.label("id"),
                db.PitchFx.pitcher_id_mlb.label("pitcher_id_mlb"),
                db.PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.sum(db.PitchFx.batter_did_swing).label("total_swings"),
                func.sum(db.PitchFx.batter_made_contact).label("total_swings_made_contact"),
                func.sum(db.PitchFx.called_strike).label("total_called_strikes"),
                func.sum(db.PitchFx.swinging_strike).label("total_swinging_strikes"),
                func.sum(db.PitchFx.inside_strike_zone).label("total_inside_strike_zone"),
                func.sum(db.PitchFx.outside_strike_zone).label("total_outside_strike_zone"),
                func.sum(db.PitchFx.swing_inside_zone).label("total_swings_inside_zone"),
                func.sum(db.PitchFx.swing_outside_zone).label("total_swings_outside_zone"),
                func.sum(db.PitchFx.contact_inside_zone).label("total_contact_inside_zone"),
                func.sum(db.PitchFx.contact_outside_zone).label("total_contact_outside_zone"),
                zone_rate,
                called_strike_rate,
                swinging_strike_rate,
                whiff_rate,
                csw_rate,
                o_swing_rate,
                z_swing_rate,
                swing_rate,
                o_contact_rate,
                z_contact_rate,
                contact_rate,
                custom_score,
                func.sum(db.PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(db.PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(db.PitchFx.is_pop_up).label("total_pop_ups"),
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
            ]
        )
        .where(
            and_(
                db.PitchFx.is_duplicate_guid == 0,
                db.PitchFx.is_duplicate_pitch_number == 0,
                db.PitchFx.is_invalid_ibb == 0,
                db.PitchFx.is_out_of_sequence == 0,
                db.PitchFx.stand == "L",
            )
        )
        .select_from(db.PitchFx)
        .group_by(db.PitchFx.pitcher_id)
        .group_by(db.PitchFx.mlbam_pitch_name)
        .order_by(db.PitchFx.pitcher_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pitchfx_metrics_vs_lhb_for_player(cls, db_engine, player_id):
        s = select([cls]).where(cls.id == player_id)
        results = db_engine.execute(s).fetchall()
        return PitchFxMetricsCollection.from_pitchfx_view_results(results) if results else None


class Pitch_Type_By_Year_View(db.Base):
    __table__ = create_view(
        name="pitch_type_by_year",
        selectable=select(
            [
                db.PitchFx.pitcher_id.label("id"),
                db.PitchFx.pitcher_id_mlb.label("pitcher_id_mlb"),
                db.PitchFx.season_id.label("season_id"),
                db.PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.sum(db.PitchFx.batter_did_swing).label("total_swings"),
                func.sum(db.PitchFx.batter_made_contact).label("total_swings_made_contact"),
                func.sum(db.PitchFx.called_strike).label("total_called_strikes"),
                func.sum(db.PitchFx.swinging_strike).label("total_swinging_strikes"),
                func.sum(db.PitchFx.inside_strike_zone).label("total_inside_strike_zone"),
                func.sum(db.PitchFx.outside_strike_zone).label("total_outside_strike_zone"),
                func.sum(db.PitchFx.swing_inside_zone).label("total_swings_inside_zone"),
                func.sum(db.PitchFx.swing_outside_zone).label("total_swings_outside_zone"),
                func.sum(db.PitchFx.contact_inside_zone).label("total_contact_inside_zone"),
                func.sum(db.PitchFx.contact_outside_zone).label("total_contact_outside_zone"),
                zone_rate,
                called_strike_rate,
                swinging_strike_rate,
                whiff_rate,
                csw_rate,
                o_swing_rate,
                z_swing_rate,
                swing_rate,
                o_contact_rate,
                z_contact_rate,
                contact_rate,
                custom_score,
                money_pitch,
                func.sum(db.PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(db.PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(db.PitchFx.is_pop_up).label("total_pop_ups"),
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
            ]
        )
        .where(
            and_(
                db.PitchFx.is_duplicate_guid == 0,
                db.PitchFx.is_duplicate_pitch_number == 0,
                db.PitchFx.is_invalid_ibb == 0,
                db.PitchFx.is_out_of_sequence == 0,
                or_(db.PitchFx.stand == "L", db.PitchFx.stand == "R"),
            )
        )
        .select_from(db.PitchFx)
        .group_by(db.PitchFx.season_id)
        .group_by(db.PitchFx.pitcher_id)
        .group_by(db.PitchFx.mlbam_pitch_name)
        .order_by(db.PitchFx.pitcher_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pitch_metrics_for_year_for_player(cls, db_engine, player_id, season_id):
        s = select([cls]).where(and_(cls.id == player_id, cls.season_id == season_id))
        results = db_engine.execute(s).fetchall()
        return PitchFxMetricsCollection.from_pitchfx_view_results(results) if results else None

    @classmethod
    def get_all_seasons_with_player_data(cls, db_engine, db_session, player_id):
        s = select([cls.season_id]).where(cls.id == player_id).distinct()
        results = db_engine.execute(s).fetchall()
        seasons_played = [db_session.query(db.Season).get(d["season_id"]) for d in [dict(row) for row in results]]
        return sorted(seasons_played, key=lambda x: x.year) if seasons_played else []


class Pitch_Type_Averages_All_View(db.Base):
    __table__ = create_view(
        name="pitch_type_avg_all",
        selectable=select(
            [
                db.PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                zone_rate,
                called_strike_rate,
                swinging_strike_rate,
                whiff_rate,
                csw_rate,
                o_swing_rate,
                z_swing_rate,
                swing_rate,
                o_contact_rate,
                z_contact_rate,
                contact_rate,
                custom_score,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
            ]
        )
        .where(
            and_(
                db.PitchFx.is_duplicate_guid == 0,
                db.PitchFx.is_duplicate_pitch_number == 0,
                db.PitchFx.is_invalid_ibb == 0,
                db.PitchFx.is_out_of_sequence == 0,
                or_(db.PitchFx.stand == "L", db.PitchFx.stand == "R"),
            )
        )
        .select_from(db.PitchFx)
        .group_by(db.PitchFx.mlbam_pitch_name)
        .order_by(db.PitchFx.mlbam_pitch_name),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_average_pitchfx_metrics(cls, db_engine):
        results = db_engine.execute(select([cls])).fetchall()
        row_dicts = [dict(row) for row in results]
        return {PitchType.from_abbrev(d["pitch_type"]): PitchFxMetrics.from_pitchfx_view_results(d) for d in row_dicts}


class Pitch_Type_Averages_Right_View(db.Base):
    __table__ = create_view(
        name="pitch_type_avg_right",
        selectable=select(
            [
                db.PitchFx.mlbam_pitch_name.label("pitch_type"),
                db.PitchFx.p_throws.label("p_throws"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                zone_rate,
                called_strike_rate,
                swinging_strike_rate,
                whiff_rate,
                csw_rate,
                o_swing_rate,
                z_swing_rate,
                swing_rate,
                o_contact_rate,
                z_contact_rate,
                contact_rate,
                custom_score,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
            ]
        )
        .where(
            and_(
                db.PitchFx.is_duplicate_guid == 0,
                db.PitchFx.is_duplicate_pitch_number == 0,
                db.PitchFx.is_invalid_ibb == 0,
                db.PitchFx.is_out_of_sequence == 0,
                db.PitchFx.p_throws == "R",
            )
        )
        .select_from(db.PitchFx)
        .group_by(db.PitchFx.mlbam_pitch_name)
        .order_by(db.PitchFx.mlbam_pitch_name),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_average_pitchfx_metrics(cls, db_engine):
        results = db_engine.execute(select([cls])).fetchall()
        row_dicts = [dict(row) for row in results]
        return {PitchType.from_abbrev(d["pitch_type"]): PitchFxMetrics.from_pitchfx_view_results(d) for d in row_dicts}


class Pitch_Type_Averages_Left_View(db.Base):
    __table__ = create_view(
        name="pitch_type_avg_left",
        selectable=select(
            [
                db.PitchFx.mlbam_pitch_name.label("pitch_type"),
                db.PitchFx.p_throws.label("p_throws"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                zone_rate,
                called_strike_rate,
                swinging_strike_rate,
                whiff_rate,
                csw_rate,
                o_swing_rate,
                z_swing_rate,
                swing_rate,
                o_contact_rate,
                z_contact_rate,
                contact_rate,
                custom_score,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
            ]
        )
        .where(
            and_(
                db.PitchFx.is_duplicate_guid == 0,
                db.PitchFx.is_duplicate_pitch_number == 0,
                db.PitchFx.is_invalid_ibb == 0,
                db.PitchFx.is_out_of_sequence == 0,
                db.PitchFx.p_throws == "L",
            )
        )
        .select_from(db.PitchFx)
        .group_by(db.PitchFx.mlbam_pitch_name)
        .order_by(db.PitchFx.mlbam_pitch_name),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_average_pitchfx_metrics(cls, db_engine):
        results = db_engine.execute(select([cls])).fetchall()
        row_dicts = [dict(row) for row in results]
        return {PitchType.from_abbrev(d["pitch_type"]): PitchFxMetrics.from_pitchfx_view_results(d) for d in row_dicts}
