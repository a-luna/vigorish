from sqlalchemy import and_, func, or_, select
from sqlalchemy_utils import create_view

from vigorish.data.metrics import PitchFxMetrics, PitchFxMetricsCollection
from vigorish.database import Base, PitchFx
from vigorish.enums import PitchType
from vigorish.models.views.pfx_col_expressions import (
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


class Pitch_Type_All_View(Base):
    __table__ = create_view(
        name="pitch_type_all",
        selectable=select(
            [
                PitchFx.pitcher_id.label("id"),
                PitchFx.pitcher_id_mlb.label("pitcher_id_mlb"),
                PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.avg(PitchFx.start_speed).label("avg_speed"),
                func.count(PitchFx.id).label("total_pitches"),
                func.sum(PitchFx.batter_did_swing).label("total_swings"),
                func.sum(PitchFx.batter_made_contact).label("total_swings_made_contact"),
                func.sum(PitchFx.called_strike).label("total_called_strikes"),
                func.sum(PitchFx.swinging_strike).label("total_swinging_strikes"),
                func.sum(PitchFx.inside_strike_zone).label("total_inside_strike_zone"),
                func.sum(PitchFx.outside_strike_zone).label("total_outside_strike_zone"),
                func.sum(PitchFx.swing_inside_zone).label("total_swings_inside_zone"),
                func.sum(PitchFx.swing_outside_zone).label("total_swings_outside_zone"),
                func.sum(PitchFx.contact_inside_zone).label("total_contact_inside_zone"),
                func.sum(PitchFx.contact_outside_zone).label("total_contact_outside_zone"),
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
                func.sum(PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(PitchFx.is_pop_up).label("total_pop_ups"),
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                func.avg(PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(PitchFx.px).label("avg_px"),
                func.avg(PitchFx.pz).label("avg_pz"),
            ]
        )
        .where(
            and_(
                PitchFx.is_duplicate_guid == 0,
                PitchFx.is_duplicate_pitch_number == 0,
                PitchFx.is_invalid_ibb == 0,
                PitchFx.is_out_of_sequence == 0,
                or_(PitchFx.stand == "L", PitchFx.stand == "R"),
            )
        )
        .select_from(PitchFx)
        .group_by(PitchFx.pitcher_id)
        .group_by(PitchFx.mlbam_pitch_name)
        .order_by(PitchFx.pitcher_id_mlb),
        metadata=Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pitchfx_metrics_for_career_for_player(cls, db_engine, player_id):
        s = select([cls]).where(cls.id == player_id)
        results = db_engine.execute(s).fetchall()
        return PitchFxMetricsCollection.from_pitchfx_view_results(results)


class Pitch_Type_Right_View(Base):
    __table__ = create_view(
        name="pitch_type_right",
        selectable=select(
            [
                PitchFx.pitcher_id.label("id"),
                PitchFx.pitcher_id_mlb.label("pitcher_id_mlb"),
                PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.avg(PitchFx.start_speed).label("avg_speed"),
                func.count(PitchFx.id).label("total_pitches"),
                func.sum(PitchFx.batter_did_swing).label("total_swings"),
                func.sum(PitchFx.batter_made_contact).label("total_swings_made_contact"),
                func.sum(PitchFx.called_strike).label("total_called_strikes"),
                func.sum(PitchFx.swinging_strike).label("total_swinging_strikes"),
                func.sum(PitchFx.inside_strike_zone).label("total_inside_strike_zone"),
                func.sum(PitchFx.outside_strike_zone).label("total_outside_strike_zone"),
                func.sum(PitchFx.swing_inside_zone).label("total_swings_inside_zone"),
                func.sum(PitchFx.swing_outside_zone).label("total_swings_outside_zone"),
                func.sum(PitchFx.contact_inside_zone).label("total_contact_inside_zone"),
                func.sum(PitchFx.contact_outside_zone).label("total_contact_outside_zone"),
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
                func.sum(PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(PitchFx.is_pop_up).label("total_pop_ups"),
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                func.avg(PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(PitchFx.px).label("avg_px"),
                func.avg(PitchFx.pz).label("avg_pz"),
            ]
        )
        .where(
            and_(
                PitchFx.is_duplicate_guid == 0,
                PitchFx.is_duplicate_pitch_number == 0,
                PitchFx.is_invalid_ibb == 0,
                PitchFx.is_out_of_sequence == 0,
                PitchFx.stand == "R",
            )
        )
        .select_from(PitchFx)
        .group_by(PitchFx.pitcher_id)
        .group_by(PitchFx.mlbam_pitch_name)
        .order_by(PitchFx.pitcher_id_mlb),
        metadata=Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pitchfx_metrics_vs_rhb_for_player(cls, db_engine, player_id):
        s = select([cls]).where(cls.id == player_id)
        results = db_engine.execute(s).fetchall()
        return PitchFxMetricsCollection.from_pitchfx_view_results(results)


class Pitch_Type_Left_View(Base):
    __table__ = create_view(
        name="pitch_type_left",
        selectable=select(
            [
                PitchFx.pitcher_id.label("id"),
                PitchFx.pitcher_id_mlb.label("pitcher_id_mlb"),
                PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.avg(PitchFx.start_speed).label("avg_speed"),
                func.count(PitchFx.id).label("total_pitches"),
                func.sum(PitchFx.batter_did_swing).label("total_swings"),
                func.sum(PitchFx.batter_made_contact).label("total_swings_made_contact"),
                func.sum(PitchFx.called_strike).label("total_called_strikes"),
                func.sum(PitchFx.swinging_strike).label("total_swinging_strikes"),
                func.sum(PitchFx.inside_strike_zone).label("total_inside_strike_zone"),
                func.sum(PitchFx.outside_strike_zone).label("total_outside_strike_zone"),
                func.sum(PitchFx.swing_inside_zone).label("total_swings_inside_zone"),
                func.sum(PitchFx.swing_outside_zone).label("total_swings_outside_zone"),
                func.sum(PitchFx.contact_inside_zone).label("total_contact_inside_zone"),
                func.sum(PitchFx.contact_outside_zone).label("total_contact_outside_zone"),
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
                func.sum(PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(PitchFx.is_pop_up).label("total_pop_ups"),
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                func.avg(PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(PitchFx.px).label("avg_px"),
                func.avg(PitchFx.pz).label("avg_pz"),
            ]
        )
        .where(
            and_(
                PitchFx.is_duplicate_guid == 0,
                PitchFx.is_duplicate_pitch_number == 0,
                PitchFx.is_invalid_ibb == 0,
                PitchFx.is_out_of_sequence == 0,
                PitchFx.stand == "L",
            )
        )
        .select_from(PitchFx)
        .group_by(PitchFx.pitcher_id)
        .group_by(PitchFx.mlbam_pitch_name)
        .order_by(PitchFx.pitcher_id_mlb),
        metadata=Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pitchfx_metrics_vs_lhb_for_player(cls, db_engine, player_id):
        s = select([cls]).where(cls.id == player_id)
        results = db_engine.execute(s).fetchall()
        return PitchFxMetricsCollection.from_pitchfx_view_results(results)


class Pitch_Type_By_Year_View(Base):
    __table__ = create_view(
        name="pitch_type_by_year",
        selectable=select(
            [
                PitchFx.pitcher_id.label("id"),
                PitchFx.pitcher_id_mlb.label("pitcher_id_mlb"),
                PitchFx.season_id.label("season_id"),
                PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.avg(PitchFx.start_speed).label("avg_speed"),
                func.count(PitchFx.id).label("total_pitches"),
                func.sum(PitchFx.batter_did_swing).label("total_swings"),
                func.sum(PitchFx.batter_made_contact).label("total_swings_made_contact"),
                func.sum(PitchFx.called_strike).label("total_called_strikes"),
                func.sum(PitchFx.swinging_strike).label("total_swinging_strikes"),
                func.sum(PitchFx.inside_strike_zone).label("total_inside_strike_zone"),
                func.sum(PitchFx.outside_strike_zone).label("total_outside_strike_zone"),
                func.sum(PitchFx.swing_inside_zone).label("total_swings_inside_zone"),
                func.sum(PitchFx.swing_outside_zone).label("total_swings_outside_zone"),
                func.sum(PitchFx.contact_inside_zone).label("total_contact_inside_zone"),
                func.sum(PitchFx.contact_outside_zone).label("total_contact_outside_zone"),
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
                func.sum(PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(PitchFx.is_pop_up).label("total_pop_ups"),
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                func.avg(PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(PitchFx.px).label("avg_px"),
                func.avg(PitchFx.pz).label("avg_pz"),
            ]
        )
        .where(
            and_(
                PitchFx.is_duplicate_guid == 0,
                PitchFx.is_duplicate_pitch_number == 0,
                PitchFx.is_invalid_ibb == 0,
                PitchFx.is_out_of_sequence == 0,
                or_(PitchFx.stand == "L", PitchFx.stand == "R"),
            )
        )
        .select_from(PitchFx)
        .group_by(PitchFx.season_id)
        .group_by(PitchFx.pitcher_id)
        .group_by(PitchFx.mlbam_pitch_name)
        .order_by(PitchFx.pitcher_id_mlb),
        metadata=Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pitch_metrics_for_year_for_player(cls, db_engine, player_id, season_id):
        s = select([cls]).where(and_(cls.id == player_id, cls.season_id == season_id))
        results = db_engine.execute(s).fetchall()
        return PitchFxMetricsCollection.from_pitchfx_view_results(results)


class Pitch_Type_Averages_All_View(Base):
    __table__ = create_view(
        name="pitch_type_avg_all",
        selectable=select(
            [
                PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.count(PitchFx.id).label("total_pitches"),
                func.avg(PitchFx.start_speed).label("avg_speed"),
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
                func.avg(PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(PitchFx.px).label("avg_px"),
                func.avg(PitchFx.pz).label("avg_pz"),
            ]
        )
        .where(
            and_(
                PitchFx.is_duplicate_guid == 0,
                PitchFx.is_duplicate_pitch_number == 0,
                PitchFx.is_invalid_ibb == 0,
                PitchFx.is_out_of_sequence == 0,
                or_(PitchFx.stand == "L", PitchFx.stand == "R"),
            )
        )
        .select_from(PitchFx)
        .group_by(PitchFx.mlbam_pitch_name)
        .order_by(PitchFx.mlbam_pitch_name),
        metadata=Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_average_pitchfx_metrics(cls, db_engine):
        results = db_engine.execute(select([cls])).fetchall()
        row_dicts = [dict(row) for row in results]
        return {PitchType.from_abbrev(d["pitch_type"]): PitchFxMetrics.from_pitchfx_view_results(d) for d in row_dicts}


class Pitch_Type_Averages_Right_View(Base):
    __table__ = create_view(
        name="pitch_type_avg_right",
        selectable=select(
            [
                PitchFx.mlbam_pitch_name.label("pitch_type"),
                PitchFx.p_throws.label("p_throws"),
                func.count(PitchFx.id).label("total_pitches"),
                func.avg(PitchFx.start_speed).label("avg_speed"),
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
                func.avg(PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(PitchFx.px).label("avg_px"),
                func.avg(PitchFx.pz).label("avg_pz"),
            ]
        )
        .where(
            and_(
                PitchFx.is_duplicate_guid == 0,
                PitchFx.is_duplicate_pitch_number == 0,
                PitchFx.is_invalid_ibb == 0,
                PitchFx.is_out_of_sequence == 0,
                PitchFx.p_throws == "R",
            )
        )
        .select_from(PitchFx)
        .group_by(PitchFx.mlbam_pitch_name)
        .order_by(PitchFx.mlbam_pitch_name),
        metadata=Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_average_pitchfx_metrics(cls, db_engine):
        results = db_engine.execute(select([cls])).fetchall()
        row_dicts = [dict(row) for row in results]
        return {PitchType.from_abbrev(d["pitch_type"]): PitchFxMetrics.from_pitchfx_view_results(d) for d in row_dicts}


class Pitch_Type_Averages_Left_View(Base):
    __table__ = create_view(
        name="pitch_type_avg_left",
        selectable=select(
            [
                PitchFx.mlbam_pitch_name.label("pitch_type"),
                PitchFx.p_throws.label("p_throws"),
                func.count(PitchFx.id).label("total_pitches"),
                func.avg(PitchFx.start_speed).label("avg_speed"),
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
                func.avg(PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(PitchFx.px).label("avg_px"),
                func.avg(PitchFx.pz).label("avg_pz"),
            ]
        )
        .where(
            and_(
                PitchFx.is_duplicate_guid == 0,
                PitchFx.is_duplicate_pitch_number == 0,
                PitchFx.is_invalid_ibb == 0,
                PitchFx.is_out_of_sequence == 0,
                PitchFx.p_throws == "L",
            )
        )
        .select_from(PitchFx)
        .group_by(PitchFx.mlbam_pitch_name)
        .order_by(PitchFx.mlbam_pitch_name),
        metadata=Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_average_pitchfx_metrics(cls, db_engine):
        results = db_engine.execute(select([cls])).fetchall()
        row_dicts = [dict(row) for row in results]
        return {PitchType.from_abbrev(d["pitch_type"]): PitchFxMetrics.from_pitchfx_view_results(d) for d in row_dicts}
