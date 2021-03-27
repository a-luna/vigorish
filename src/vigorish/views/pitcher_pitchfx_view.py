from sqlalchemy import and_, func, join, literal, or_, select
from sqlalchemy_utils import create_view

import vigorish.database as db
from vigorish.views.pfx_col_expressions import (
    avg,
    bb_rate,
    called_strike_rate,
    contact_rate,
    csw_rate,
    fly_ball_rate,
    ground_ball_rate,
    hr_per_fb,
    iso,
    k_rate,
    line_drive_rate,
    o_contact_rate,
    o_swing_rate,
    obp,
    ops,
    pop_up_rate,
    slg,
    swing_rate,
    swinging_strike_rate,
    total_at_bats,
    whiff_rate,
    z_contact_rate,
    z_swing_rate,
    zone_rate,
)


class Pitcher_PitchFx_All_View(db.Base):
    __table__ = create_view(
        name="pitcher_pitchfx_all",
        selectable=select(
            [
                db.PitchFx.pitcher_id.label("id"),
                db.PitchFx.pitcher_id_mlb.label("mlb_id"),
                db.PitchFx.p_throws.label("p_throws"),
                literal("ALL").label("pitch_type"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.sum(db.PitchFx.is_final_pitch_of_ab).label("total_pa"),
                total_at_bats,
                func.sum(db.PitchFx.ab_result_out).label("total_outs"),
                func.sum(db.PitchFx.ab_result_hit).label("total_hits"),
                func.sum(db.PitchFx.ab_result_bb).label("total_bb"),
                func.sum(db.PitchFx.ab_result_k).label("total_k"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                bb_rate,
                k_rate,
                hr_per_fb,
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
                func.sum(db.PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(db.PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(db.PitchFx.is_pop_up).label("total_pop_ups"),
                func.sum(db.PitchFx.ab_result_single).label("total_singles"),
                func.sum(db.PitchFx.ab_result_double).label("total_doubles"),
                func.sum(db.PitchFx.ab_result_triple).label("total_triples"),
                func.sum(db.PitchFx.ab_result_homerun).label("total_homeruns"),
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                func.sum(db.PitchFx.ab_result_hbp).label("total_hbp"),
                func.sum(db.PitchFx.ab_result_error).label("total_errors"),
                func.sum(db.PitchFx.ab_result_sac_hit).label("total_sac_hit"),
                func.sum(db.PitchFx.ab_result_sac_fly).label("total_sac_fly"),
                func.sum(db.PitchFx.ab_result_unclear).label("total_unclear_results"),
                func.sum(db.PitchFx.pitch_type_int.distinct()).label("pitch_type_int"),
            ]
        )
        .where(
            and_(
                db.PitchFx.is_invalid_ibb == 0,
                db.PitchFx.is_out_of_sequence == 0,
                or_(db.PitchFx.stand == "L", db.PitchFx.stand == "R"),
            )
        )
        .select_from(db.PitchFx)
        .group_by(db.PitchFx.pitcher_id)
        .order_by(db.PitchFx.pitcher_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pfx_metrics_for_career_for_pitcher(cls, db_engine, mlb_id):
        results = db_engine.execute(select([cls]).where(cls.mlb_id == mlb_id)).fetchall()
        return [dict(row) for row in results][0] if results else {}


class Pitcher_PitchType_All_View(db.Base):
    __table__ = create_view(
        name="pitcher_pitchtype_all",
        selectable=select(
            [
                db.PitchFx.pitcher_id.label("id"),
                db.PitchFx.pitcher_id_mlb.label("mlb_id"),
                db.PitchFx.p_throws.label("p_throws"),
                db.PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.sum(db.PitchFx.is_final_pitch_of_ab).label("total_pa"),
                total_at_bats,
                func.sum(db.PitchFx.ab_result_out).label("total_outs"),
                func.sum(db.PitchFx.ab_result_hit).label("total_hits"),
                func.sum(db.PitchFx.ab_result_bb).label("total_bb"),
                func.sum(db.PitchFx.ab_result_k).label("total_k"),
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                bb_rate,
                k_rate,
                hr_per_fb,
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
                func.sum(db.PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(db.PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(db.PitchFx.is_pop_up).label("total_pop_ups"),
                func.sum(db.PitchFx.ab_result_single).label("total_singles"),
                func.sum(db.PitchFx.ab_result_double).label("total_doubles"),
                func.sum(db.PitchFx.ab_result_triple).label("total_triples"),
                func.sum(db.PitchFx.ab_result_homerun).label("total_homeruns"),
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                func.sum(db.PitchFx.ab_result_hbp).label("total_hbp"),
                func.sum(db.PitchFx.ab_result_error).label("total_errors"),
                func.sum(db.PitchFx.ab_result_sac_hit).label("total_sac_hit"),
                func.sum(db.PitchFx.ab_result_sac_fly).label("total_sac_fly"),
                func.sum(db.PitchFx.ab_result_unclear).label("total_unclear_results"),
            ]
        )
        .where(
            and_(
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
    def get_pfx_metrics_for_career_for_pitcher(cls, db_engine, mlb_id):
        results = db_engine.execute(select([cls]).where(cls.mlb_id == mlb_id)).fetchall()
        return [dict(row) for row in results] if results else []


class Pitcher_PitchFx_vs_RHB_View(db.Base):
    __table__ = create_view(
        name="pitcher_pitchfx_right",
        selectable=select(
            [
                db.PitchFx.pitcher_id.label("id"),
                db.PitchFx.pitcher_id_mlb.label("mlb_id"),
                db.PitchFx.p_throws.label("p_throws"),
                literal("ALL").label("pitch_type"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.sum(db.PitchFx.is_final_pitch_of_ab).label("total_pa"),
                total_at_bats,
                func.sum(db.PitchFx.ab_result_out).label("total_outs"),
                func.sum(db.PitchFx.ab_result_hit).label("total_hits"),
                func.sum(db.PitchFx.ab_result_bb).label("total_bb"),
                func.sum(db.PitchFx.ab_result_k).label("total_k"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                bb_rate,
                k_rate,
                hr_per_fb,
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
                func.sum(db.PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(db.PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(db.PitchFx.is_pop_up).label("total_pop_ups"),
                func.sum(db.PitchFx.ab_result_single).label("total_singles"),
                func.sum(db.PitchFx.ab_result_double).label("total_doubles"),
                func.sum(db.PitchFx.ab_result_triple).label("total_triples"),
                func.sum(db.PitchFx.ab_result_homerun).label("total_homeruns"),
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                func.sum(db.PitchFx.ab_result_hbp).label("total_hbp"),
                func.sum(db.PitchFx.ab_result_error).label("total_errors"),
                func.sum(db.PitchFx.ab_result_sac_hit).label("total_sac_hit"),
                func.sum(db.PitchFx.ab_result_sac_fly).label("total_sac_fly"),
                func.sum(db.PitchFx.ab_result_unclear).label("total_unclear_results"),
                func.sum(db.PitchFx.pitch_type_int.distinct()).label("pitch_type_int"),
            ]
        )
        .where(
            and_(
                db.PitchFx.is_invalid_ibb == 0,
                db.PitchFx.is_out_of_sequence == 0,
                db.PitchFx.stand == "R",
            )
        )
        .select_from(db.PitchFx)
        .group_by(db.PitchFx.pitcher_id)
        .order_by(db.PitchFx.pitcher_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pfx_metrics_vs_rhb_for_pitcher(cls, db_engine, mlb_id):
        results = db_engine.execute(select([cls]).where(cls.mlb_id == mlb_id)).fetchall()
        return [dict(row) for row in results][0] if results else {}


class Pitcher_PitchType_vs_RHB_View(db.Base):
    __table__ = create_view(
        name="pitcher_pitchtype_right",
        selectable=select(
            [
                db.PitchFx.pitcher_id.label("id"),
                db.PitchFx.pitcher_id_mlb.label("mlb_id"),
                db.PitchFx.p_throws.label("p_throws"),
                db.PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.sum(db.PitchFx.is_final_pitch_of_ab).label("total_pa"),
                total_at_bats,
                func.sum(db.PitchFx.ab_result_out).label("total_outs"),
                func.sum(db.PitchFx.ab_result_hit).label("total_hits"),
                func.sum(db.PitchFx.ab_result_bb).label("total_bb"),
                func.sum(db.PitchFx.ab_result_k).label("total_k"),
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                bb_rate,
                k_rate,
                hr_per_fb,
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
                func.sum(db.PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(db.PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(db.PitchFx.is_pop_up).label("total_pop_ups"),
                func.sum(db.PitchFx.ab_result_single).label("total_singles"),
                func.sum(db.PitchFx.ab_result_double).label("total_doubles"),
                func.sum(db.PitchFx.ab_result_triple).label("total_triples"),
                func.sum(db.PitchFx.ab_result_homerun).label("total_homeruns"),
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                func.sum(db.PitchFx.ab_result_hbp).label("total_hbp"),
                func.sum(db.PitchFx.ab_result_error).label("total_errors"),
                func.sum(db.PitchFx.ab_result_sac_hit).label("total_sac_hit"),
                func.sum(db.PitchFx.ab_result_sac_fly).label("total_sac_fly"),
                func.sum(db.PitchFx.ab_result_unclear).label("total_unclear_results"),
            ]
        )
        .where(
            and_(
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
    def get_pfx_metrics_vs_rhb_for_pitcher(cls, db_engine, mlb_id):
        results = db_engine.execute(select([cls]).where(cls.mlb_id == mlb_id)).fetchall()
        return [dict(row) for row in results] if results else []


class Pitcher_PitchFx_vs_LHB_View(db.Base):
    __table__ = create_view(
        name="pitcher_pitchfx_left",
        selectable=select(
            [
                db.PitchFx.pitcher_id.label("id"),
                db.PitchFx.pitcher_id_mlb.label("mlb_id"),
                db.PitchFx.p_throws.label("p_throws"),
                literal("ALL").label("pitch_type"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.sum(db.PitchFx.is_final_pitch_of_ab).label("total_pa"),
                total_at_bats,
                func.sum(db.PitchFx.ab_result_out).label("total_outs"),
                func.sum(db.PitchFx.ab_result_hit).label("total_hits"),
                func.sum(db.PitchFx.ab_result_bb).label("total_bb"),
                func.sum(db.PitchFx.ab_result_k).label("total_k"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                bb_rate,
                k_rate,
                hr_per_fb,
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
                func.sum(db.PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(db.PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(db.PitchFx.is_pop_up).label("total_pop_ups"),
                func.sum(db.PitchFx.ab_result_single).label("total_singles"),
                func.sum(db.PitchFx.ab_result_double).label("total_doubles"),
                func.sum(db.PitchFx.ab_result_triple).label("total_triples"),
                func.sum(db.PitchFx.ab_result_homerun).label("total_homeruns"),
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                func.sum(db.PitchFx.ab_result_hbp).label("total_hbp"),
                func.sum(db.PitchFx.ab_result_error).label("total_errors"),
                func.sum(db.PitchFx.ab_result_sac_hit).label("total_sac_hit"),
                func.sum(db.PitchFx.ab_result_sac_fly).label("total_sac_fly"),
                func.sum(db.PitchFx.ab_result_unclear).label("total_unclear_results"),
                func.sum(db.PitchFx.pitch_type_int.distinct()).label("pitch_type_int"),
            ]
        )
        .where(
            and_(
                db.PitchFx.is_invalid_ibb == 0,
                db.PitchFx.is_out_of_sequence == 0,
                db.PitchFx.stand == "L",
            )
        )
        .select_from(db.PitchFx)
        .group_by(db.PitchFx.pitcher_id)
        .order_by(db.PitchFx.pitcher_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pfx_metrics_vs_lhb_for_pitcher(cls, db_engine, mlb_id):
        results = db_engine.execute(select([cls]).where(cls.mlb_id == mlb_id)).fetchall()
        return [dict(row) for row in results][0] if results else {}


class Pitcher_PitchType_vs_LHB_View(db.Base):
    __table__ = create_view(
        name="pitcher_pitchtype_left",
        selectable=select(
            [
                db.PitchFx.pitcher_id.label("id"),
                db.PitchFx.pitcher_id_mlb.label("mlb_id"),
                db.PitchFx.p_throws.label("p_throws"),
                db.PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.sum(db.PitchFx.is_final_pitch_of_ab).label("total_pa"),
                total_at_bats,
                func.sum(db.PitchFx.ab_result_out).label("total_outs"),
                func.sum(db.PitchFx.ab_result_hit).label("total_hits"),
                func.sum(db.PitchFx.ab_result_bb).label("total_bb"),
                func.sum(db.PitchFx.ab_result_k).label("total_k"),
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                bb_rate,
                k_rate,
                hr_per_fb,
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
                func.sum(db.PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(db.PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(db.PitchFx.is_pop_up).label("total_pop_ups"),
                func.sum(db.PitchFx.ab_result_single).label("total_singles"),
                func.sum(db.PitchFx.ab_result_double).label("total_doubles"),
                func.sum(db.PitchFx.ab_result_triple).label("total_triples"),
                func.sum(db.PitchFx.ab_result_homerun).label("total_homeruns"),
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                func.sum(db.PitchFx.ab_result_hbp).label("total_hbp"),
                func.sum(db.PitchFx.ab_result_error).label("total_errors"),
                func.sum(db.PitchFx.ab_result_sac_hit).label("total_sac_hit"),
                func.sum(db.PitchFx.ab_result_sac_fly).label("total_sac_fly"),
                func.sum(db.PitchFx.ab_result_unclear).label("total_unclear_results"),
            ]
        )
        .where(
            and_(
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
    def get_pfx_metrics_vs_lhb_for_pitcher(cls, db_engine, mlb_id):
        results = db_engine.execute(select([cls]).where(cls.mlb_id == mlb_id)).fetchall()
        return [dict(row) for row in results] if results else []


class Pitcher_PitchFx_All_By_Year_View(db.Base):
    __table__ = create_view(
        name="pitcher_pitchfx_all_by_year",
        selectable=select(
            [
                db.PitchFx.pitcher_id.label("id"),
                db.PitchFx.pitcher_id_mlb.label("mlb_id"),
                db.PitchFx.p_throws.label("p_throws"),
                db.Season.year.label("year"),
                literal("ALL").label("pitch_type"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.sum(db.PitchFx.is_final_pitch_of_ab).label("total_pa"),
                total_at_bats,
                func.sum(db.PitchFx.ab_result_out).label("total_outs"),
                func.sum(db.PitchFx.ab_result_hit).label("total_hits"),
                func.sum(db.PitchFx.ab_result_bb).label("total_bb"),
                func.sum(db.PitchFx.ab_result_k).label("total_k"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                bb_rate,
                k_rate,
                hr_per_fb,
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
                func.sum(db.PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(db.PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(db.PitchFx.is_pop_up).label("total_pop_ups"),
                func.sum(db.PitchFx.ab_result_single).label("total_singles"),
                func.sum(db.PitchFx.ab_result_double).label("total_doubles"),
                func.sum(db.PitchFx.ab_result_triple).label("total_triples"),
                func.sum(db.PitchFx.ab_result_homerun).label("total_homeruns"),
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                func.sum(db.PitchFx.ab_result_hbp).label("total_hbp"),
                func.sum(db.PitchFx.ab_result_error).label("total_errors"),
                func.sum(db.PitchFx.ab_result_sac_hit).label("total_sac_hit"),
                func.sum(db.PitchFx.ab_result_sac_fly).label("total_sac_fly"),
                func.sum(db.PitchFx.ab_result_unclear).label("total_unclear_results"),
                func.sum(db.PitchFx.pitch_type_int.distinct()).label("pitch_type_int"),
                db.PitchFx.season_id.label("season_id"),
            ]
        )
        .where(
            and_(
                db.PitchFx.is_invalid_ibb == 0,
                db.PitchFx.is_out_of_sequence == 0,
                or_(db.PitchFx.stand == "L", db.PitchFx.stand == "R"),
            )
        )
        .select_from(join(db.PitchFx, db.Season, db.PitchFx.season_id == db.Season.id))
        .group_by(db.PitchFx.season_id)
        .group_by(db.PitchFx.pitcher_id)
        .order_by(db.PitchFx.pitcher_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pfx_metrics_by_year_for_pitcher(cls, db_engine, mlb_id, year):
        s = select([cls]).where(and_(cls.mlb_id == mlb_id, cls.year == year))
        results = db_engine.execute(s).fetchall()
        return [dict(row) for row in results][0] if results else {}


class Pitcher_PitchType_All_By_Year_View(db.Base):
    __table__ = create_view(
        name="pitcher_pitchtype_all_by_year",
        selectable=select(
            [
                db.PitchFx.pitcher_id.label("id"),
                db.PitchFx.pitcher_id_mlb.label("mlb_id"),
                db.PitchFx.p_throws.label("p_throws"),
                db.Season.year.label("year"),
                db.PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.sum(db.PitchFx.is_final_pitch_of_ab).label("total_pa"),
                total_at_bats,
                func.sum(db.PitchFx.ab_result_out).label("total_outs"),
                func.sum(db.PitchFx.ab_result_hit).label("total_hits"),
                func.sum(db.PitchFx.ab_result_bb).label("total_bb"),
                func.sum(db.PitchFx.ab_result_k).label("total_k"),
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                bb_rate,
                k_rate,
                hr_per_fb,
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
                func.sum(db.PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(db.PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(db.PitchFx.is_pop_up).label("total_pop_ups"),
                func.sum(db.PitchFx.ab_result_single).label("total_singles"),
                func.sum(db.PitchFx.ab_result_double).label("total_doubles"),
                func.sum(db.PitchFx.ab_result_triple).label("total_triples"),
                func.sum(db.PitchFx.ab_result_homerun).label("total_homeruns"),
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                func.sum(db.PitchFx.ab_result_hbp).label("total_hbp"),
                func.sum(db.PitchFx.ab_result_error).label("total_errors"),
                func.sum(db.PitchFx.ab_result_sac_hit).label("total_sac_hit"),
                func.sum(db.PitchFx.ab_result_sac_fly).label("total_sac_fly"),
                func.sum(db.PitchFx.ab_result_unclear).label("total_unclear_results"),
                db.PitchFx.season_id.label("season_id"),
            ]
        )
        .where(
            and_(
                db.PitchFx.is_invalid_ibb == 0,
                db.PitchFx.is_out_of_sequence == 0,
                or_(db.PitchFx.stand == "L", db.PitchFx.stand == "R"),
            )
        )
        .select_from(join(db.PitchFx, db.Season, db.PitchFx.season_id == db.Season.id))
        .group_by(db.PitchFx.season_id)
        .group_by(db.PitchFx.pitcher_id)
        .group_by(db.PitchFx.mlbam_pitch_name)
        .order_by(db.PitchFx.pitcher_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pfx_metrics_by_year_for_pitcher(cls, db_engine, mlb_id, year):
        s = select([cls]).where(and_(cls.mlb_id == mlb_id, cls.year == year))
        results = db_engine.execute(s).fetchall()
        return [dict(row) for row in results] if results else []


class Pitcher_PitchFx_vs_RHB_By_Year_View(db.Base):
    __table__ = create_view(
        name="pitcher_pitchfx_vs_rhb_by_year",
        selectable=select(
            [
                db.PitchFx.pitcher_id.label("id"),
                db.PitchFx.pitcher_id_mlb.label("mlb_id"),
                db.PitchFx.p_throws.label("p_throws"),
                db.Season.year.label("year"),
                literal("ALL").label("pitch_type"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.sum(db.PitchFx.is_final_pitch_of_ab).label("total_pa"),
                total_at_bats,
                func.sum(db.PitchFx.ab_result_out).label("total_outs"),
                func.sum(db.PitchFx.ab_result_hit).label("total_hits"),
                func.sum(db.PitchFx.ab_result_bb).label("total_bb"),
                func.sum(db.PitchFx.ab_result_k).label("total_k"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                bb_rate,
                k_rate,
                hr_per_fb,
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
                func.sum(db.PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(db.PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(db.PitchFx.is_pop_up).label("total_pop_ups"),
                func.sum(db.PitchFx.ab_result_single).label("total_singles"),
                func.sum(db.PitchFx.ab_result_double).label("total_doubles"),
                func.sum(db.PitchFx.ab_result_triple).label("total_triples"),
                func.sum(db.PitchFx.ab_result_homerun).label("total_homeruns"),
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                func.sum(db.PitchFx.ab_result_hbp).label("total_hbp"),
                func.sum(db.PitchFx.ab_result_error).label("total_errors"),
                func.sum(db.PitchFx.ab_result_sac_hit).label("total_sac_hit"),
                func.sum(db.PitchFx.ab_result_sac_fly).label("total_sac_fly"),
                func.sum(db.PitchFx.ab_result_unclear).label("total_unclear_results"),
                func.sum(db.PitchFx.pitch_type_int.distinct()).label("pitch_type_int"),
                db.PitchFx.season_id.label("season_id"),
            ]
        )
        .where(
            and_(
                db.PitchFx.is_invalid_ibb == 0,
                db.PitchFx.is_out_of_sequence == 0,
                db.PitchFx.stand == "R",
            )
        )
        .select_from(join(db.PitchFx, db.Season, db.PitchFx.season_id == db.Season.id))
        .group_by(db.PitchFx.season_id)
        .group_by(db.PitchFx.pitcher_id)
        .order_by(db.PitchFx.pitcher_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pfx_metrics_by_year_for_pitcher(cls, db_engine, mlb_id, year):
        s = select([cls]).where(and_(cls.mlb_id == mlb_id, cls.year == year))
        results = db_engine.execute(s).fetchall()
        return [dict(row) for row in results][0] if results else {}


class Pitcher_PitchType_vs_RHB_By_Year_View(db.Base):
    __table__ = create_view(
        name="pitcher_pitchtype_vs_rhb_by_year",
        selectable=select(
            [
                db.PitchFx.pitcher_id.label("id"),
                db.PitchFx.pitcher_id_mlb.label("mlb_id"),
                db.PitchFx.p_throws.label("p_throws"),
                db.Season.year.label("year"),
                db.PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.sum(db.PitchFx.is_final_pitch_of_ab).label("total_pa"),
                total_at_bats,
                func.sum(db.PitchFx.ab_result_out).label("total_outs"),
                func.sum(db.PitchFx.ab_result_hit).label("total_hits"),
                func.sum(db.PitchFx.ab_result_bb).label("total_bb"),
                func.sum(db.PitchFx.ab_result_k).label("total_k"),
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                bb_rate,
                k_rate,
                hr_per_fb,
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
                func.sum(db.PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(db.PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(db.PitchFx.is_pop_up).label("total_pop_ups"),
                func.sum(db.PitchFx.ab_result_single).label("total_singles"),
                func.sum(db.PitchFx.ab_result_double).label("total_doubles"),
                func.sum(db.PitchFx.ab_result_triple).label("total_triples"),
                func.sum(db.PitchFx.ab_result_homerun).label("total_homeruns"),
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                func.sum(db.PitchFx.ab_result_hbp).label("total_hbp"),
                func.sum(db.PitchFx.ab_result_error).label("total_errors"),
                func.sum(db.PitchFx.ab_result_sac_hit).label("total_sac_hit"),
                func.sum(db.PitchFx.ab_result_sac_fly).label("total_sac_fly"),
                func.sum(db.PitchFx.ab_result_unclear).label("total_unclear_results"),
                db.PitchFx.season_id.label("season_id"),
            ]
        )
        .where(
            and_(
                db.PitchFx.is_invalid_ibb == 0,
                db.PitchFx.is_out_of_sequence == 0,
                db.PitchFx.stand == "R",
            )
        )
        .select_from(join(db.PitchFx, db.Season, db.PitchFx.season_id == db.Season.id))
        .group_by(db.PitchFx.season_id)
        .group_by(db.PitchFx.pitcher_id)
        .group_by(db.PitchFx.mlbam_pitch_name)
        .order_by(db.PitchFx.pitcher_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pfx_metrics_by_year_for_pitcher(cls, db_engine, mlb_id, year):
        s = select([cls]).where(and_(cls.mlb_id == mlb_id, cls.year == year))
        results = db_engine.execute(s).fetchall()
        return [dict(row) for row in results] if results else []


class Pitcher_PitchFx_vs_LHB_By_Year_View(db.Base):
    __table__ = create_view(
        name="pitcher_pitchfx_vs_lhb_by_year",
        selectable=select(
            [
                db.PitchFx.pitcher_id.label("id"),
                db.PitchFx.pitcher_id_mlb.label("mlb_id"),
                db.PitchFx.p_throws.label("p_throws"),
                db.Season.year.label("year"),
                literal("ALL").label("pitch_type"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.sum(db.PitchFx.is_final_pitch_of_ab).label("total_pa"),
                total_at_bats,
                func.sum(db.PitchFx.ab_result_out).label("total_outs"),
                func.sum(db.PitchFx.ab_result_hit).label("total_hits"),
                func.sum(db.PitchFx.ab_result_bb).label("total_bb"),
                func.sum(db.PitchFx.ab_result_k).label("total_k"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                bb_rate,
                k_rate,
                hr_per_fb,
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
                func.sum(db.PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(db.PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(db.PitchFx.is_pop_up).label("total_pop_ups"),
                func.sum(db.PitchFx.ab_result_single).label("total_singles"),
                func.sum(db.PitchFx.ab_result_double).label("total_doubles"),
                func.sum(db.PitchFx.ab_result_triple).label("total_triples"),
                func.sum(db.PitchFx.ab_result_homerun).label("total_homeruns"),
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                func.sum(db.PitchFx.ab_result_hbp).label("total_hbp"),
                func.sum(db.PitchFx.ab_result_error).label("total_errors"),
                func.sum(db.PitchFx.ab_result_sac_hit).label("total_sac_hit"),
                func.sum(db.PitchFx.ab_result_sac_fly).label("total_sac_fly"),
                func.sum(db.PitchFx.ab_result_unclear).label("total_unclear_results"),
                func.sum(db.PitchFx.pitch_type_int.distinct()).label("pitch_type_int"),
                db.PitchFx.season_id.label("season_id"),
            ]
        )
        .where(
            and_(
                db.PitchFx.is_invalid_ibb == 0,
                db.PitchFx.is_out_of_sequence == 0,
                db.PitchFx.stand == "L",
            )
        )
        .select_from(join(db.PitchFx, db.Season, db.PitchFx.season_id == db.Season.id))
        .group_by(db.PitchFx.season_id)
        .group_by(db.PitchFx.pitcher_id)
        .order_by(db.PitchFx.pitcher_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pfx_metrics_by_year_for_pitcher(cls, db_engine, mlb_id, year):
        s = select([cls]).where(and_(cls.mlb_id == mlb_id, cls.year == year))
        results = db_engine.execute(s).fetchall()
        return [dict(row) for row in results][0] if results else {}


class Pitcher_PitchType_vs_LHB_By_Year_View(db.Base):
    __table__ = create_view(
        name="pitcher_pitchtype_vs_lhb_by_year",
        selectable=select(
            [
                db.PitchFx.pitcher_id.label("id"),
                db.PitchFx.pitcher_id_mlb.label("mlb_id"),
                db.PitchFx.p_throws.label("p_throws"),
                db.Season.year.label("year"),
                db.PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.sum(db.PitchFx.is_final_pitch_of_ab).label("total_pa"),
                total_at_bats,
                func.sum(db.PitchFx.ab_result_out).label("total_outs"),
                func.sum(db.PitchFx.ab_result_hit).label("total_hits"),
                func.sum(db.PitchFx.ab_result_bb).label("total_bb"),
                func.sum(db.PitchFx.ab_result_k).label("total_k"),
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                bb_rate,
                k_rate,
                hr_per_fb,
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
                func.sum(db.PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(db.PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(db.PitchFx.is_pop_up).label("total_pop_ups"),
                func.sum(db.PitchFx.ab_result_single).label("total_singles"),
                func.sum(db.PitchFx.ab_result_double).label("total_doubles"),
                func.sum(db.PitchFx.ab_result_triple).label("total_triples"),
                func.sum(db.PitchFx.ab_result_homerun).label("total_homeruns"),
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                func.sum(db.PitchFx.ab_result_hbp).label("total_hbp"),
                func.sum(db.PitchFx.ab_result_error).label("total_errors"),
                func.sum(db.PitchFx.ab_result_sac_hit).label("total_sac_hit"),
                func.sum(db.PitchFx.ab_result_sac_fly).label("total_sac_fly"),
                func.sum(db.PitchFx.ab_result_unclear).label("total_unclear_results"),
                db.PitchFx.season_id.label("season_id"),
            ]
        )
        .where(
            and_(
                db.PitchFx.is_invalid_ibb == 0,
                db.PitchFx.is_out_of_sequence == 0,
                db.PitchFx.stand == "L",
            )
        )
        .select_from(join(db.PitchFx, db.Season, db.PitchFx.season_id == db.Season.id))
        .group_by(db.PitchFx.season_id)
        .group_by(db.PitchFx.pitcher_id)
        .group_by(db.PitchFx.mlbam_pitch_name)
        .order_by(db.PitchFx.pitcher_id_mlb),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pfx_metrics_by_year_for_pitcher(cls, db_engine, mlb_id, year):
        s = select([cls]).where(and_(cls.mlb_id == mlb_id, cls.year == year))
        results = db_engine.execute(s).fetchall()
        return [dict(row) for row in results] if results else []


class Pitcher_PitchFx_For_Game_All_View(db.Base):
    __table__ = create_view(
        name="pitcher_pitchfx_for_game_all",
        selectable=select(
            [
                db.PitchFx.pitcher_id.label("id"),
                db.PitchFx.pitcher_id_mlb.label("mlb_id"),
                db.PitchFx.p_throws.label("p_throws"),
                db.PitchFx.pitch_app_id.label("pitch_app_id"),
                db.PitchFx.bbref_game_id.label("bbref_game_id"),
                literal("ALL").label("pitch_type"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.sum(db.PitchFx.is_final_pitch_of_ab).label("total_pa"),
                total_at_bats,
                func.sum(db.PitchFx.ab_result_out).label("total_outs"),
                func.sum(db.PitchFx.ab_result_hit).label("total_hits"),
                func.sum(db.PitchFx.ab_result_bb).label("total_bb"),
                func.sum(db.PitchFx.ab_result_k).label("total_k"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                bb_rate,
                k_rate,
                hr_per_fb,
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
                func.sum(db.PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(db.PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(db.PitchFx.is_pop_up).label("total_pop_ups"),
                func.sum(db.PitchFx.ab_result_single).label("total_singles"),
                func.sum(db.PitchFx.ab_result_double).label("total_doubles"),
                func.sum(db.PitchFx.ab_result_triple).label("total_triples"),
                func.sum(db.PitchFx.ab_result_homerun).label("total_homeruns"),
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                func.sum(db.PitchFx.ab_result_hbp).label("total_hbp"),
                func.sum(db.PitchFx.ab_result_error).label("total_errors"),
                func.sum(db.PitchFx.ab_result_sac_hit).label("total_sac_hit"),
                func.sum(db.PitchFx.ab_result_sac_fly).label("total_sac_fly"),
                func.sum(db.PitchFx.ab_result_unclear).label("total_unclear_results"),
                func.sum(db.PitchFx.pitch_type_int.distinct()).label("pitch_type_int"),
                db.PitchFx.team_pitching_id.label("team_pitching_id"),
                db.PitchFx.team_batting_id.label("team_batting_id"),
                db.PitchFx.pitch_app_db_id.label("pitch_app_db_id"),
                db.PitchFx.game_status_id.label("game_status_id"),
                db.PitchFx.date_id.label("date_id"),
                db.PitchFx.season_id.label("season_id"),
            ]
        )
        .where(
            and_(
                db.PitchFx.is_invalid_ibb == 0,
                db.PitchFx.is_out_of_sequence == 0,
                or_(db.PitchFx.stand == "L", db.PitchFx.stand == "R"),
            )
        )
        .select_from(db.PitchFx)
        .group_by(db.PitchFx.pitcher_id)
        .group_by(db.PitchFx.game_status_id)
        .order_by(db.PitchFx.pitcher_id_mlb)
        .order_by(db.PitchFx.date_id),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pfx_metrics_for_game_for_pitcher(cls, db_engine, mlb_id, game_id):
        s = select([cls]).where(and_(cls.mlb_id == mlb_id, cls.bbref_game_id == game_id))
        results = db_engine.execute(s).fetchall()
        return [dict(row) for row in results][0] if results else {}


class Pitcher_PitchType_For_Game_All_View(db.Base):
    __table__ = create_view(
        name="pitcher_pitchtype_for_game_all",
        selectable=select(
            [
                db.PitchFx.pitcher_id.label("id"),
                db.PitchFx.pitcher_id_mlb.label("mlb_id"),
                db.PitchFx.p_throws.label("p_throws"),
                db.PitchFx.pitch_app_id.label("pitch_app_id"),
                db.PitchFx.bbref_game_id.label("bbref_game_id"),
                db.PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.sum(db.PitchFx.is_final_pitch_of_ab).label("total_pa"),
                total_at_bats,
                func.sum(db.PitchFx.ab_result_out).label("total_outs"),
                func.sum(db.PitchFx.ab_result_hit).label("total_hits"),
                func.sum(db.PitchFx.ab_result_bb).label("total_bb"),
                func.sum(db.PitchFx.ab_result_k).label("total_k"),
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                bb_rate,
                k_rate,
                hr_per_fb,
                zone_rate,
                called_strike_rate,
                swinging_strike_rate,
                whiff_rate,
                csw_rate,
                o_swing_rate,
                z_swing_rate,
                swing_rate,
                o_contact_rate,
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
                func.sum(db.PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(db.PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(db.PitchFx.is_pop_up).label("total_pop_ups"),
                func.sum(db.PitchFx.ab_result_single).label("total_singles"),
                func.sum(db.PitchFx.ab_result_double).label("total_doubles"),
                func.sum(db.PitchFx.ab_result_triple).label("total_triples"),
                func.sum(db.PitchFx.ab_result_homerun).label("total_homeruns"),
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                func.sum(db.PitchFx.ab_result_hbp).label("total_hbp"),
                func.sum(db.PitchFx.ab_result_error).label("total_errors"),
                func.sum(db.PitchFx.ab_result_sac_hit).label("total_sac_hit"),
                func.sum(db.PitchFx.ab_result_sac_fly).label("total_sac_fly"),
                func.sum(db.PitchFx.ab_result_unclear).label("total_unclear_results"),
                func.sum(db.PitchFx.pitch_type_int.distinct()).label("pitch_type_int"),
                db.PitchFx.team_pitching_id.label("team_pitching_id"),
                db.PitchFx.team_batting_id.label("team_batting_id"),
                db.PitchFx.pitch_app_db_id.label("pitch_app_db_id"),
                db.PitchFx.game_status_id.label("game_status_id"),
                db.PitchFx.date_id.label("date_id"),
                db.PitchFx.season_id.label("season_id"),
            ]
        )
        .where(
            and_(
                db.PitchFx.is_invalid_ibb == 0,
                db.PitchFx.is_out_of_sequence == 0,
                or_(db.PitchFx.stand == "L", db.PitchFx.stand == "R"),
            )
        )
        .select_from(db.PitchFx)
        .group_by(db.PitchFx.pitcher_id)
        .group_by(db.PitchFx.game_status_id)
        .group_by(db.PitchFx.mlbam_pitch_name)
        .order_by(db.PitchFx.pitcher_id_mlb)
        .order_by(db.PitchFx.date_id),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pfx_metrics_for_game_for_pitcher(cls, db_engine, mlb_id, game_id):
        s = select([cls]).where(and_(cls.mlb_id == mlb_id, cls.bbref_game_id == game_id))
        results = db_engine.execute(s).fetchall()
        return [dict(row) for row in results] if results else []


class Pitcher_PitchFx_For_Game_vs_RHB_View(db.Base):
    __table__ = create_view(
        name="pitcher_pitchfx_for_game_vs_rhb",
        selectable=select(
            [
                db.PitchFx.pitcher_id.label("id"),
                db.PitchFx.pitcher_id_mlb.label("mlb_id"),
                db.PitchFx.p_throws.label("p_throws"),
                db.PitchFx.pitch_app_id.label("pitch_app_id"),
                db.PitchFx.bbref_game_id.label("bbref_game_id"),
                literal("ALL").label("pitch_type"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.sum(db.PitchFx.is_final_pitch_of_ab).label("total_pa"),
                total_at_bats,
                func.sum(db.PitchFx.ab_result_out).label("total_outs"),
                func.sum(db.PitchFx.ab_result_hit).label("total_hits"),
                func.sum(db.PitchFx.ab_result_bb).label("total_bb"),
                func.sum(db.PitchFx.ab_result_k).label("total_k"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                bb_rate,
                k_rate,
                hr_per_fb,
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
                func.sum(db.PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(db.PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(db.PitchFx.is_pop_up).label("total_pop_ups"),
                func.sum(db.PitchFx.ab_result_single).label("total_singles"),
                func.sum(db.PitchFx.ab_result_double).label("total_doubles"),
                func.sum(db.PitchFx.ab_result_triple).label("total_triples"),
                func.sum(db.PitchFx.ab_result_homerun).label("total_homeruns"),
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                func.sum(db.PitchFx.ab_result_hbp).label("total_hbp"),
                func.sum(db.PitchFx.ab_result_error).label("total_errors"),
                func.sum(db.PitchFx.ab_result_sac_hit).label("total_sac_hit"),
                func.sum(db.PitchFx.ab_result_sac_fly).label("total_sac_fly"),
                func.sum(db.PitchFx.ab_result_unclear).label("total_unclear_results"),
                func.sum(db.PitchFx.pitch_type_int.distinct()).label("pitch_type_int"),
                db.PitchFx.team_pitching_id.label("team_pitching_id"),
                db.PitchFx.team_batting_id.label("team_batting_id"),
                db.PitchFx.pitch_app_db_id.label("pitch_app_db_id"),
                db.PitchFx.game_status_id.label("game_status_id"),
                db.PitchFx.date_id.label("date_id"),
                db.PitchFx.season_id.label("season_id"),
            ]
        )
        .where(
            and_(
                db.PitchFx.is_invalid_ibb == 0,
                db.PitchFx.is_out_of_sequence == 0,
                db.PitchFx.stand == "R",
            )
        )
        .select_from(db.PitchFx)
        .group_by(db.PitchFx.pitcher_id)
        .group_by(db.PitchFx.game_status_id)
        .order_by(db.PitchFx.pitcher_id_mlb)
        .order_by(db.PitchFx.date_id),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pfx_metrics_for_game_vs_rhb_for_pitcher(cls, db_engine, mlb_id, game_id):
        s = select([cls]).where(and_(cls.mlb_id == mlb_id, cls.bbref_game_id == game_id))
        results = db_engine.execute(s).fetchall()
        return [dict(row) for row in results][0] if results else {}


class Pitcher_PitchType_For_Game_vs_RHB_View(db.Base):
    __table__ = create_view(
        name="pitcher_pitchtype_for_game_vs_rhb",
        selectable=select(
            [
                db.PitchFx.pitcher_id.label("id"),
                db.PitchFx.pitcher_id_mlb.label("mlb_id"),
                db.PitchFx.p_throws.label("p_throws"),
                db.PitchFx.pitch_app_id.label("pitch_app_id"),
                db.PitchFx.bbref_game_id.label("bbref_game_id"),
                db.PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.sum(db.PitchFx.is_final_pitch_of_ab).label("total_pa"),
                total_at_bats,
                func.sum(db.PitchFx.ab_result_out).label("total_outs"),
                func.sum(db.PitchFx.ab_result_hit).label("total_hits"),
                func.sum(db.PitchFx.ab_result_bb).label("total_bb"),
                func.sum(db.PitchFx.ab_result_k).label("total_k"),
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                bb_rate,
                k_rate,
                hr_per_fb,
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
                func.sum(db.PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(db.PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(db.PitchFx.is_pop_up).label("total_pop_ups"),
                func.sum(db.PitchFx.ab_result_single).label("total_singles"),
                func.sum(db.PitchFx.ab_result_double).label("total_doubles"),
                func.sum(db.PitchFx.ab_result_triple).label("total_triples"),
                func.sum(db.PitchFx.ab_result_homerun).label("total_homeruns"),
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                func.sum(db.PitchFx.ab_result_hbp).label("total_hbp"),
                func.sum(db.PitchFx.ab_result_error).label("total_errors"),
                func.sum(db.PitchFx.ab_result_sac_hit).label("total_sac_hit"),
                func.sum(db.PitchFx.ab_result_sac_fly).label("total_sac_fly"),
                func.sum(db.PitchFx.ab_result_unclear).label("total_unclear_results"),
                func.sum(db.PitchFx.pitch_type_int.distinct()).label("pitch_type_int"),
                db.PitchFx.team_pitching_id.label("team_pitching_id"),
                db.PitchFx.team_batting_id.label("team_batting_id"),
                db.PitchFx.pitch_app_db_id.label("pitch_app_db_id"),
                db.PitchFx.game_status_id.label("game_status_id"),
                db.PitchFx.date_id.label("date_id"),
                db.PitchFx.season_id.label("season_id"),
            ]
        )
        .where(
            and_(
                db.PitchFx.is_invalid_ibb == 0,
                db.PitchFx.is_out_of_sequence == 0,
                db.PitchFx.stand == "R",
            )
        )
        .select_from(db.PitchFx)
        .group_by(db.PitchFx.pitcher_id)
        .group_by(db.PitchFx.game_status_id)
        .group_by(db.PitchFx.mlbam_pitch_name)
        .order_by(db.PitchFx.pitcher_id_mlb)
        .order_by(db.PitchFx.date_id),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pfx_metrics_for_game_vs_rhb_for_pitcher(cls, db_engine, mlb_id, game_id):
        s = select([cls]).where(and_(cls.mlb_id == mlb_id, cls.bbref_game_id == game_id))
        results = db_engine.execute(s).fetchall()
        return [dict(row) for row in results] if results else []


class Pitcher_PitchFx_For_Game_vs_LHB_View(db.Base):
    __table__ = create_view(
        name="pitcher_pitchfx_for_game_vs_lhb",
        selectable=select(
            [
                db.PitchFx.pitcher_id.label("id"),
                db.PitchFx.pitcher_id_mlb.label("mlb_id"),
                db.PitchFx.p_throws.label("p_throws"),
                db.PitchFx.pitch_app_id.label("pitch_app_id"),
                db.PitchFx.bbref_game_id.label("bbref_game_id"),
                literal("ALL").label("pitch_type"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.sum(db.PitchFx.is_final_pitch_of_ab).label("total_pa"),
                total_at_bats,
                func.sum(db.PitchFx.ab_result_out).label("total_outs"),
                func.sum(db.PitchFx.ab_result_hit).label("total_hits"),
                func.sum(db.PitchFx.ab_result_bb).label("total_bb"),
                func.sum(db.PitchFx.ab_result_k).label("total_k"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                bb_rate,
                k_rate,
                hr_per_fb,
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
                func.sum(db.PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(db.PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(db.PitchFx.is_pop_up).label("total_pop_ups"),
                func.sum(db.PitchFx.ab_result_single).label("total_singles"),
                func.sum(db.PitchFx.ab_result_double).label("total_doubles"),
                func.sum(db.PitchFx.ab_result_triple).label("total_triples"),
                func.sum(db.PitchFx.ab_result_homerun).label("total_homeruns"),
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                func.sum(db.PitchFx.ab_result_hbp).label("total_hbp"),
                func.sum(db.PitchFx.ab_result_error).label("total_errors"),
                func.sum(db.PitchFx.ab_result_sac_hit).label("total_sac_hit"),
                func.sum(db.PitchFx.ab_result_sac_fly).label("total_sac_fly"),
                func.sum(db.PitchFx.ab_result_unclear).label("total_unclear_results"),
                func.sum(db.PitchFx.pitch_type_int.distinct()).label("pitch_type_int"),
                db.PitchFx.team_pitching_id.label("team_pitching_id"),
                db.PitchFx.team_batting_id.label("team_batting_id"),
                db.PitchFx.pitch_app_db_id.label("pitch_app_db_id"),
                db.PitchFx.game_status_id.label("game_status_id"),
                db.PitchFx.date_id.label("date_id"),
                db.PitchFx.season_id.label("season_id"),
            ]
        )
        .where(
            and_(
                db.PitchFx.is_invalid_ibb == 0,
                db.PitchFx.is_out_of_sequence == 0,
                db.PitchFx.stand == "L",
            )
        )
        .select_from(db.PitchFx)
        .group_by(db.PitchFx.pitcher_id)
        .group_by(db.PitchFx.game_status_id)
        .order_by(db.PitchFx.pitcher_id_mlb)
        .order_by(db.PitchFx.date_id),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pfx_metrics_for_game_vs_lhb_for_pitcher(cls, db_engine, mlb_id, game_id):
        s = select([cls]).where(and_(cls.mlb_id == mlb_id, cls.bbref_game_id == game_id))
        results = db_engine.execute(s).fetchall()
        return [dict(row) for row in results][0] if results else {}


class Pitcher_PitchType_For_Game_vs_LHB_View(db.Base):
    __table__ = create_view(
        name="pitcher_pitchtype_for_game_vs_lhb",
        selectable=select(
            [
                db.PitchFx.pitcher_id.label("id"),
                db.PitchFx.pitcher_id_mlb.label("mlb_id"),
                db.PitchFx.p_throws.label("p_throws"),
                db.PitchFx.pitch_app_id.label("pitch_app_id"),
                db.PitchFx.bbref_game_id.label("bbref_game_id"),
                db.PitchFx.mlbam_pitch_name.label("pitch_type"),
                func.count(db.PitchFx.id).label("total_pitches"),
                func.sum(db.PitchFx.is_final_pitch_of_ab).label("total_pa"),
                total_at_bats,
                func.sum(db.PitchFx.ab_result_out).label("total_outs"),
                func.sum(db.PitchFx.ab_result_hit).label("total_hits"),
                func.sum(db.PitchFx.ab_result_bb).label("total_bb"),
                func.sum(db.PitchFx.ab_result_k).label("total_k"),
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                pop_up_rate,
                bb_rate,
                k_rate,
                hr_per_fb,
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
                func.sum(db.PitchFx.is_batted_ball).label("total_batted_balls"),
                func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls"),
                func.sum(db.PitchFx.is_line_drive).label("total_line_drives"),
                func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls"),
                func.sum(db.PitchFx.is_pop_up).label("total_pop_ups"),
                func.sum(db.PitchFx.ab_result_single).label("total_singles"),
                func.sum(db.PitchFx.ab_result_double).label("total_doubles"),
                func.sum(db.PitchFx.ab_result_triple).label("total_triples"),
                func.sum(db.PitchFx.ab_result_homerun).label("total_homeruns"),
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                func.sum(db.PitchFx.ab_result_hbp).label("total_hbp"),
                func.sum(db.PitchFx.ab_result_error).label("total_errors"),
                func.sum(db.PitchFx.ab_result_sac_hit).label("total_sac_hit"),
                func.sum(db.PitchFx.ab_result_sac_fly).label("total_sac_fly"),
                func.sum(db.PitchFx.ab_result_unclear).label("total_unclear_results"),
                func.sum(db.PitchFx.pitch_type_int.distinct()).label("pitch_type_int"),
                db.PitchFx.team_pitching_id.label("team_pitching_id"),
                db.PitchFx.team_batting_id.label("team_batting_id"),
                db.PitchFx.pitch_app_db_id.label("pitch_app_db_id"),
                db.PitchFx.game_status_id.label("game_status_id"),
                db.PitchFx.date_id.label("date_id"),
                db.PitchFx.season_id.label("season_id"),
            ]
        )
        .where(
            and_(
                db.PitchFx.is_invalid_ibb == 0,
                db.PitchFx.is_out_of_sequence == 0,
                db.PitchFx.stand == "L",
            )
        )
        .select_from(db.PitchFx)
        .group_by(db.PitchFx.pitcher_id)
        .group_by(db.PitchFx.game_status_id)
        .group_by(db.PitchFx.mlbam_pitch_name)
        .order_by(db.PitchFx.pitcher_id_mlb)
        .order_by(db.PitchFx.date_id),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_pfx_metrics_for_game_vs_lhb_for_pitcher(cls, db_engine, mlb_id, game_id):
        s = select([cls]).where(and_(cls.mlb_id == mlb_id, cls.bbref_game_id == game_id))
        results = db_engine.execute(s).fetchall()
        return [dict(row) for row in results] if results else []
