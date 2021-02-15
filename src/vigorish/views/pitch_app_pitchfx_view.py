from sqlalchemy import and_, func, or_, select
from sqlalchemy_utils import create_view

import vigorish.database as db
from vigorish.data.metrics import PitchFxMetricsCollection
from vigorish.views.pfx_col_expressions import (
    called_strike_rate,
    contact_rate,
    csw_rate,
    fly_ball_rate,
    ground_ball_rate,
    line_drive_rate,
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


class PitchApp_PitchType_All_View(db.Base):
    __table__ = create_view(
        name="pitch_app_pitch_type_all",
        selectable=select(
            [
                db.PitchFx.pitch_app_db_id.label("id"),
                db.PitchFx.pitcher_id.label("pitcher_id"),
                db.PitchFx.pitcher_id_mlb.label("pitcher_id_mlb"),
                db.PitchFx.pitch_app_id.label("pitch_app_id"),
                db.PitchFx.bbref_game_id.label("bbref_game_id"),
                db.PitchFx.team_pitching_id.label("team_pitching_id"),
                db.PitchFx.team_batting_id.label("team_batting_id"),
                db.PitchFx.game_status_id.label("game_status_id"),
                db.PitchFx.date_id.label("date_id"),
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
        .group_by(db.PitchFx.pitch_app_db_id)
        .group_by(db.PitchFx.mlbam_pitch_name)
        .order_by(db.PitchFx.pitch_app_id),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pitchfx_metrics_for_pitch_app(cls, db_engine, pitch_app_id):
        s = select([cls]).where(cls.id == pitch_app_id)
        results = db_engine.execute(s).fetchall()
        return PitchFxMetricsCollection.from_pitchfx_view_results(results)


class PitchApp_PitchType_Right_View(db.Base):
    __table__ = create_view(
        name="pitch_app_pitch_type_right",
        selectable=select(
            [
                db.PitchFx.pitch_app_db_id.label("id"),
                db.PitchFx.pitcher_id.label("pitcher_id"),
                db.PitchFx.pitcher_id_mlb.label("pitcher_id_mlb"),
                db.PitchFx.pitch_app_id.label("pitch_app_id"),
                db.PitchFx.bbref_game_id.label("bbref_game_id"),
                db.PitchFx.team_pitching_id.label("team_pitching_id"),
                db.PitchFx.team_batting_id.label("team_batting_id"),
                db.PitchFx.game_status_id.label("game_status_id"),
                db.PitchFx.date_id.label("date_id"),
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
        .group_by(db.PitchFx.pitch_app_db_id)
        .group_by(db.PitchFx.mlbam_pitch_name)
        .order_by(db.PitchFx.pitch_app_id),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pitchfx_metrics_vs_rhb_for_pitch_app(cls, db_engine, pitch_app_id):
        s = select([cls]).where(cls.id == pitch_app_id)
        results = db_engine.execute(s).fetchall()
        return PitchFxMetricsCollection.from_pitchfx_view_results(results)


class PitchApp_PitchType_Left_View(db.Base):
    __table__ = create_view(
        name="pitch_app_pitch_type_left",
        selectable=select(
            [
                db.PitchFx.pitch_app_db_id.label("id"),
                db.PitchFx.pitcher_id.label("pitcher_id"),
                db.PitchFx.pitcher_id_mlb.label("pitcher_id_mlb"),
                db.PitchFx.pitch_app_id.label("pitch_app_id"),
                db.PitchFx.bbref_game_id.label("bbref_game_id"),
                db.PitchFx.team_pitching_id.label("team_pitching_id"),
                db.PitchFx.team_batting_id.label("team_batting_id"),
                db.PitchFx.game_status_id.label("game_status_id"),
                db.PitchFx.date_id.label("date_id"),
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
        .group_by(db.PitchFx.pitch_app_db_id)
        .group_by(db.PitchFx.mlbam_pitch_name)
        .order_by(db.PitchFx.pitch_app_id),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pitchfx_metrics_vs_lhb_for_pitch_app(cls, db_engine, pitch_app_id):
        s = select([cls]).where(cls.id == pitch_app_id)
        results = db_engine.execute(s).fetchall()
        return PitchFxMetricsCollection.from_pitchfx_view_results(results)
