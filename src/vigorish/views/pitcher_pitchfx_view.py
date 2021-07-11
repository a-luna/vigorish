from sqlalchemy import and_, func, join, literal, or_, select
from sqlalchemy_utils import create_view

import vigorish.database as db
from vigorish.views.pfx_col_expressions import (
    avg,
    avg_launch_speed,
    avg_launch_angle,
    avg_hit_distance,
    barrel_rate,
    bb_rate,
    called_strike_rate,
    contact_rate,
    csw_rate,
    fly_ball_rate,
    ground_ball_rate,
    hard_hit_rate,
    hr_per_fb,
    iso,
    k_rate,
    line_drive_rate,
    medium_hit_rate,
    o_contact_rate,
    o_swing_rate,
    obp,
    ops,
    popup_rate,
    slg,
    soft_hit_rate,
    swing_rate,
    swinging_strike_rate,
    total_at_bats,
    total_balls_in_play,
    total_barrels,
    total_bb,
    total_called_strikes,
    total_contact_inside_zone,
    total_contact_outside_zone,
    total_doubles,
    total_errors,
    total_fly_balls,
    total_ground_balls,
    total_hard_hits,
    total_hbp,
    total_hits,
    total_homeruns,
    total_k,
    total_line_drives,
    total_medium_hits,
    total_outs,
    total_pa,
    total_pitches,
    total_inside_strike_zone,
    total_outside_strike_zone,
    total_popups,
    total_sac_hit,
    total_sac_fly,
    total_singles,
    total_soft_hits,
    total_swinging_strikes,
    total_swings,
    total_swings_inside_zone,
    total_swings_made_contact,
    total_swings_outside_zone,
    total_triples,
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
                total_pitches,
                total_pa,
                total_at_bats,
                total_outs,
                total_hits,
                total_bb,
                total_k,
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                popup_rate,
                hard_hit_rate,
                medium_hit_rate,
                soft_hit_rate,
                barrel_rate,
                avg_launch_speed,
                avg_launch_angle,
                avg_hit_distance,
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
                total_swings,
                total_swings_made_contact,
                total_called_strikes,
                total_swinging_strikes,
                total_inside_strike_zone,
                total_outside_strike_zone,
                total_swings_inside_zone,
                total_swings_outside_zone,
                total_contact_inside_zone,
                total_contact_outside_zone,
                total_balls_in_play,
                total_ground_balls,
                total_line_drives,
                total_fly_balls,
                total_popups,
                total_hard_hits,
                total_medium_hits,
                total_soft_hits,
                total_barrels,
                total_singles,
                total_doubles,
                total_triples,
                total_homeruns,
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                total_hbp,
                total_errors,
                total_sac_hit,
                total_sac_fly,
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
                total_pitches,
                total_pa,
                total_at_bats,
                total_outs,
                total_hits,
                total_bb,
                total_k,
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
                func.avg(db.PitchFx.plate_time).label("avg_plate_time"),
                func.avg(db.PitchFx.extension).label("avg_extension"),
                func.avg(db.PitchFx.break_angle).label("avg_break_angle"),
                func.avg(db.PitchFx.break_length).label("avg_break_length"),
                func.avg(db.PitchFx.break_y).label("avg_break_y"),
                func.avg(db.PitchFx.spin_rate).label("avg_spin_rate"),
                func.avg(db.PitchFx.spin_direction).label("avg_spin_direction"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                popup_rate,
                hard_hit_rate,
                medium_hit_rate,
                soft_hit_rate,
                barrel_rate,
                avg_launch_speed,
                avg_launch_angle,
                avg_hit_distance,
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
                total_swings,
                total_swings_made_contact,
                total_called_strikes,
                total_swinging_strikes,
                total_inside_strike_zone,
                total_outside_strike_zone,
                total_swings_inside_zone,
                total_swings_outside_zone,
                total_contact_inside_zone,
                total_contact_outside_zone,
                total_balls_in_play,
                total_ground_balls,
                total_line_drives,
                total_fly_balls,
                total_popups,
                total_hard_hits,
                total_medium_hits,
                total_soft_hits,
                total_barrels,
                total_singles,
                total_doubles,
                total_triples,
                total_homeruns,
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                total_hbp,
                total_errors,
                total_sac_hit,
                total_sac_fly,
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
                total_pitches,
                total_pa,
                total_at_bats,
                total_outs,
                total_hits,
                total_bb,
                total_k,
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                popup_rate,
                hard_hit_rate,
                medium_hit_rate,
                soft_hit_rate,
                barrel_rate,
                avg_launch_speed,
                avg_launch_angle,
                avg_hit_distance,
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
                total_swings,
                total_swings_made_contact,
                total_called_strikes,
                total_swinging_strikes,
                total_inside_strike_zone,
                total_outside_strike_zone,
                total_swings_inside_zone,
                total_swings_outside_zone,
                total_contact_inside_zone,
                total_contact_outside_zone,
                total_balls_in_play,
                total_ground_balls,
                total_line_drives,
                total_fly_balls,
                total_popups,
                total_hard_hits,
                total_medium_hits,
                total_soft_hits,
                total_barrels,
                total_singles,
                total_doubles,
                total_triples,
                total_homeruns,
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                total_hbp,
                total_errors,
                total_sac_hit,
                total_sac_fly,
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
                total_pitches,
                total_pa,
                total_at_bats,
                total_outs,
                total_hits,
                total_bb,
                total_k,
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
                func.avg(db.PitchFx.plate_time).label("avg_plate_time"),
                func.avg(db.PitchFx.extension).label("avg_extension"),
                func.avg(db.PitchFx.break_angle).label("avg_break_angle"),
                func.avg(db.PitchFx.break_length).label("avg_break_length"),
                func.avg(db.PitchFx.break_y).label("avg_break_y"),
                func.avg(db.PitchFx.spin_rate).label("avg_spin_rate"),
                func.avg(db.PitchFx.spin_direction).label("avg_spin_direction"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                popup_rate,
                hard_hit_rate,
                medium_hit_rate,
                soft_hit_rate,
                barrel_rate,
                avg_launch_speed,
                avg_launch_angle,
                avg_hit_distance,
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
                total_swings,
                total_swings_made_contact,
                total_called_strikes,
                total_swinging_strikes,
                total_inside_strike_zone,
                total_outside_strike_zone,
                total_swings_inside_zone,
                total_swings_outside_zone,
                total_contact_inside_zone,
                total_contact_outside_zone,
                total_balls_in_play,
                total_ground_balls,
                total_line_drives,
                total_fly_balls,
                total_popups,
                total_hard_hits,
                total_medium_hits,
                total_soft_hits,
                total_barrels,
                total_singles,
                total_doubles,
                total_triples,
                total_homeruns,
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                total_hbp,
                total_errors,
                total_sac_hit,
                total_sac_fly,
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
                total_pitches,
                total_pa,
                total_at_bats,
                total_outs,
                total_hits,
                total_bb,
                total_k,
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                popup_rate,
                hard_hit_rate,
                medium_hit_rate,
                soft_hit_rate,
                barrel_rate,
                avg_launch_speed,
                avg_launch_angle,
                avg_hit_distance,
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
                total_swings,
                total_swings_made_contact,
                total_called_strikes,
                total_swinging_strikes,
                total_inside_strike_zone,
                total_outside_strike_zone,
                total_swings_inside_zone,
                total_swings_outside_zone,
                total_contact_inside_zone,
                total_contact_outside_zone,
                total_balls_in_play,
                total_ground_balls,
                total_line_drives,
                total_fly_balls,
                total_popups,
                total_hard_hits,
                total_medium_hits,
                total_soft_hits,
                total_barrels,
                total_singles,
                total_doubles,
                total_triples,
                total_homeruns,
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                total_hbp,
                total_errors,
                total_sac_hit,
                total_sac_fly,
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
                total_pitches,
                total_pa,
                total_at_bats,
                total_outs,
                total_hits,
                total_bb,
                total_k,
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
                func.avg(db.PitchFx.plate_time).label("avg_plate_time"),
                func.avg(db.PitchFx.extension).label("avg_extension"),
                func.avg(db.PitchFx.break_angle).label("avg_break_angle"),
                func.avg(db.PitchFx.break_length).label("avg_break_length"),
                func.avg(db.PitchFx.break_y).label("avg_break_y"),
                func.avg(db.PitchFx.spin_rate).label("avg_spin_rate"),
                func.avg(db.PitchFx.spin_direction).label("avg_spin_direction"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                popup_rate,
                hard_hit_rate,
                medium_hit_rate,
                soft_hit_rate,
                barrel_rate,
                avg_launch_speed,
                avg_launch_angle,
                avg_hit_distance,
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
                total_swings,
                total_swings_made_contact,
                total_called_strikes,
                total_swinging_strikes,
                total_inside_strike_zone,
                total_outside_strike_zone,
                total_swings_inside_zone,
                total_swings_outside_zone,
                total_contact_inside_zone,
                total_contact_outside_zone,
                total_balls_in_play,
                total_ground_balls,
                total_line_drives,
                total_fly_balls,
                total_popups,
                total_hard_hits,
                total_medium_hits,
                total_soft_hits,
                total_barrels,
                total_singles,
                total_doubles,
                total_triples,
                total_homeruns,
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                total_hbp,
                total_errors,
                total_sac_hit,
                total_sac_fly,
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
                total_pitches,
                total_pa,
                total_at_bats,
                total_outs,
                total_hits,
                total_bb,
                total_k,
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                popup_rate,
                hard_hit_rate,
                medium_hit_rate,
                soft_hit_rate,
                barrel_rate,
                avg_launch_speed,
                avg_launch_angle,
                avg_hit_distance,
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
                total_swings,
                total_swings_made_contact,
                total_called_strikes,
                total_swinging_strikes,
                total_inside_strike_zone,
                total_outside_strike_zone,
                total_swings_inside_zone,
                total_swings_outside_zone,
                total_contact_inside_zone,
                total_contact_outside_zone,
                total_balls_in_play,
                total_ground_balls,
                total_line_drives,
                total_fly_balls,
                total_popups,
                total_hard_hits,
                total_medium_hits,
                total_soft_hits,
                total_barrels,
                total_singles,
                total_doubles,
                total_triples,
                total_homeruns,
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                total_hbp,
                total_errors,
                total_sac_hit,
                total_sac_fly,
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
                total_pitches,
                total_pa,
                total_at_bats,
                total_outs,
                total_hits,
                total_bb,
                total_k,
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
                func.avg(db.PitchFx.plate_time).label("avg_plate_time"),
                func.avg(db.PitchFx.extension).label("avg_extension"),
                func.avg(db.PitchFx.break_angle).label("avg_break_angle"),
                func.avg(db.PitchFx.break_length).label("avg_break_length"),
                func.avg(db.PitchFx.break_y).label("avg_break_y"),
                func.avg(db.PitchFx.spin_rate).label("avg_spin_rate"),
                func.avg(db.PitchFx.spin_direction).label("avg_spin_direction"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                popup_rate,
                hard_hit_rate,
                medium_hit_rate,
                soft_hit_rate,
                barrel_rate,
                avg_launch_speed,
                avg_launch_angle,
                avg_hit_distance,
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
                total_swings,
                total_swings_made_contact,
                total_called_strikes,
                total_swinging_strikes,
                total_inside_strike_zone,
                total_outside_strike_zone,
                total_swings_inside_zone,
                total_swings_outside_zone,
                total_contact_inside_zone,
                total_contact_outside_zone,
                total_balls_in_play,
                total_ground_balls,
                total_line_drives,
                total_fly_balls,
                total_popups,
                total_hard_hits,
                total_medium_hits,
                total_soft_hits,
                total_barrels,
                total_singles,
                total_doubles,
                total_triples,
                total_homeruns,
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                total_hbp,
                total_errors,
                total_sac_hit,
                total_sac_fly,
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
                total_pitches,
                total_pa,
                total_at_bats,
                total_outs,
                total_hits,
                total_bb,
                total_k,
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                popup_rate,
                hard_hit_rate,
                medium_hit_rate,
                soft_hit_rate,
                barrel_rate,
                avg_launch_speed,
                avg_launch_angle,
                avg_hit_distance,
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
                total_swings,
                total_swings_made_contact,
                total_called_strikes,
                total_swinging_strikes,
                total_inside_strike_zone,
                total_outside_strike_zone,
                total_swings_inside_zone,
                total_swings_outside_zone,
                total_contact_inside_zone,
                total_contact_outside_zone,
                total_balls_in_play,
                total_ground_balls,
                total_line_drives,
                total_fly_balls,
                total_popups,
                total_hard_hits,
                total_medium_hits,
                total_soft_hits,
                total_barrels,
                total_singles,
                total_doubles,
                total_triples,
                total_homeruns,
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                total_hbp,
                total_errors,
                total_sac_hit,
                total_sac_fly,
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
                total_pitches,
                total_pa,
                total_at_bats,
                total_outs,
                total_hits,
                total_bb,
                total_k,
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
                func.avg(db.PitchFx.plate_time).label("avg_plate_time"),
                func.avg(db.PitchFx.extension).label("avg_extension"),
                func.avg(db.PitchFx.break_angle).label("avg_break_angle"),
                func.avg(db.PitchFx.break_length).label("avg_break_length"),
                func.avg(db.PitchFx.break_y).label("avg_break_y"),
                func.avg(db.PitchFx.spin_rate).label("avg_spin_rate"),
                func.avg(db.PitchFx.spin_direction).label("avg_spin_direction"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                popup_rate,
                hard_hit_rate,
                medium_hit_rate,
                soft_hit_rate,
                barrel_rate,
                avg_launch_speed,
                avg_launch_angle,
                avg_hit_distance,
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
                total_swings,
                total_swings_made_contact,
                total_called_strikes,
                total_swinging_strikes,
                total_inside_strike_zone,
                total_outside_strike_zone,
                total_swings_inside_zone,
                total_swings_outside_zone,
                total_contact_inside_zone,
                total_contact_outside_zone,
                total_balls_in_play,
                total_ground_balls,
                total_line_drives,
                total_fly_balls,
                total_popups,
                total_hard_hits,
                total_medium_hits,
                total_soft_hits,
                total_barrels,
                total_singles,
                total_doubles,
                total_triples,
                total_homeruns,
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                total_hbp,
                total_errors,
                total_sac_hit,
                total_sac_fly,
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
                total_pitches,
                total_pa,
                total_at_bats,
                total_outs,
                total_hits,
                total_bb,
                total_k,
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                popup_rate,
                hard_hit_rate,
                medium_hit_rate,
                soft_hit_rate,
                barrel_rate,
                avg_launch_speed,
                avg_launch_angle,
                avg_hit_distance,
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
                total_swings,
                total_swings_made_contact,
                total_called_strikes,
                total_swinging_strikes,
                total_inside_strike_zone,
                total_outside_strike_zone,
                total_swings_inside_zone,
                total_swings_outside_zone,
                total_contact_inside_zone,
                total_contact_outside_zone,
                total_balls_in_play,
                total_ground_balls,
                total_line_drives,
                total_fly_balls,
                total_popups,
                total_hard_hits,
                total_medium_hits,
                total_soft_hits,
                total_barrels,
                total_singles,
                total_doubles,
                total_triples,
                total_homeruns,
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                total_hbp,
                total_errors,
                total_sac_hit,
                total_sac_fly,
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
                total_pitches,
                total_pa,
                total_at_bats,
                total_outs,
                total_hits,
                total_bb,
                total_k,
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
                func.avg(db.PitchFx.plate_time).label("avg_plate_time"),
                func.avg(db.PitchFx.extension).label("avg_extension"),
                func.avg(db.PitchFx.break_angle).label("avg_break_angle"),
                func.avg(db.PitchFx.break_length).label("avg_break_length"),
                func.avg(db.PitchFx.break_y).label("avg_break_y"),
                func.avg(db.PitchFx.spin_rate).label("avg_spin_rate"),
                func.avg(db.PitchFx.spin_direction).label("avg_spin_direction"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                popup_rate,
                hard_hit_rate,
                medium_hit_rate,
                soft_hit_rate,
                barrel_rate,
                avg_launch_speed,
                avg_launch_angle,
                avg_hit_distance,
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
                total_swings,
                total_swings_made_contact,
                total_called_strikes,
                total_swinging_strikes,
                total_inside_strike_zone,
                total_outside_strike_zone,
                total_swings_inside_zone,
                total_swings_outside_zone,
                total_contact_inside_zone,
                total_contact_outside_zone,
                total_balls_in_play,
                total_ground_balls,
                total_line_drives,
                total_fly_balls,
                total_popups,
                total_hard_hits,
                total_medium_hits,
                total_soft_hits,
                total_barrels,
                total_singles,
                total_doubles,
                total_triples,
                total_homeruns,
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                total_hbp,
                total_errors,
                total_sac_hit,
                total_sac_fly,
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
                total_pitches,
                total_pa,
                total_at_bats,
                total_outs,
                total_hits,
                total_bb,
                total_k,
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                popup_rate,
                hard_hit_rate,
                medium_hit_rate,
                soft_hit_rate,
                barrel_rate,
                avg_launch_speed,
                avg_launch_angle,
                avg_hit_distance,
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
                total_swings,
                total_swings_made_contact,
                total_called_strikes,
                total_swinging_strikes,
                total_inside_strike_zone,
                total_outside_strike_zone,
                total_swings_inside_zone,
                total_swings_outside_zone,
                total_contact_inside_zone,
                total_contact_outside_zone,
                total_balls_in_play,
                total_ground_balls,
                total_line_drives,
                total_fly_balls,
                total_popups,
                total_hard_hits,
                total_medium_hits,
                total_soft_hits,
                total_barrels,
                total_singles,
                total_doubles,
                total_triples,
                total_homeruns,
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                total_hbp,
                total_errors,
                total_sac_hit,
                total_sac_fly,
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
                total_pitches,
                total_pa,
                total_at_bats,
                total_outs,
                total_hits,
                total_bb,
                total_k,
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
                func.avg(db.PitchFx.plate_time).label("avg_plate_time"),
                func.avg(db.PitchFx.extension).label("avg_extension"),
                func.avg(db.PitchFx.break_angle).label("avg_break_angle"),
                func.avg(db.PitchFx.break_length).label("avg_break_length"),
                func.avg(db.PitchFx.break_y).label("avg_break_y"),
                func.avg(db.PitchFx.spin_rate).label("avg_spin_rate"),
                func.avg(db.PitchFx.spin_direction).label("avg_spin_direction"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                popup_rate,
                hard_hit_rate,
                medium_hit_rate,
                soft_hit_rate,
                barrel_rate,
                avg_launch_speed,
                avg_launch_angle,
                avg_hit_distance,
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
                total_swings,
                total_swings_made_contact,
                total_called_strikes,
                total_swinging_strikes,
                total_inside_strike_zone,
                total_outside_strike_zone,
                total_swings_inside_zone,
                total_swings_outside_zone,
                total_contact_inside_zone,
                total_contact_outside_zone,
                total_balls_in_play,
                total_ground_balls,
                total_line_drives,
                total_fly_balls,
                total_popups,
                total_hard_hits,
                total_medium_hits,
                total_soft_hits,
                total_barrels,
                total_singles,
                total_doubles,
                total_triples,
                total_homeruns,
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                total_hbp,
                total_errors,
                total_sac_hit,
                total_sac_fly,
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
                total_pitches,
                total_pa,
                total_at_bats,
                total_outs,
                total_hits,
                total_bb,
                total_k,
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                popup_rate,
                hard_hit_rate,
                medium_hit_rate,
                soft_hit_rate,
                barrel_rate,
                avg_launch_speed,
                avg_launch_angle,
                avg_hit_distance,
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
                total_swings,
                total_swings_made_contact,
                total_called_strikes,
                total_swinging_strikes,
                total_inside_strike_zone,
                total_outside_strike_zone,
                total_swings_inside_zone,
                total_swings_outside_zone,
                total_contact_inside_zone,
                total_contact_outside_zone,
                total_balls_in_play,
                total_ground_balls,
                total_line_drives,
                total_fly_balls,
                total_popups,
                total_hard_hits,
                total_medium_hits,
                total_soft_hits,
                total_barrels,
                total_singles,
                total_doubles,
                total_triples,
                total_homeruns,
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                total_hbp,
                total_errors,
                total_sac_hit,
                total_sac_fly,
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
                total_pitches,
                total_pa,
                total_at_bats,
                total_outs,
                total_hits,
                total_bb,
                total_k,
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
                func.avg(db.PitchFx.plate_time).label("avg_plate_time"),
                func.avg(db.PitchFx.extension).label("avg_extension"),
                func.avg(db.PitchFx.break_angle).label("avg_break_angle"),
                func.avg(db.PitchFx.break_length).label("avg_break_length"),
                func.avg(db.PitchFx.break_y).label("avg_break_y"),
                func.avg(db.PitchFx.spin_rate).label("avg_spin_rate"),
                func.avg(db.PitchFx.spin_direction).label("avg_spin_direction"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                popup_rate,
                hard_hit_rate,
                medium_hit_rate,
                soft_hit_rate,
                barrel_rate,
                avg_launch_speed,
                avg_launch_angle,
                avg_hit_distance,
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
                total_swings,
                total_swings_made_contact,
                total_called_strikes,
                total_swinging_strikes,
                total_inside_strike_zone,
                total_outside_strike_zone,
                total_swings_inside_zone,
                total_swings_outside_zone,
                total_contact_inside_zone,
                total_contact_outside_zone,
                total_balls_in_play,
                total_ground_balls,
                total_line_drives,
                total_fly_balls,
                total_popups,
                total_hard_hits,
                total_medium_hits,
                total_soft_hits,
                total_barrels,
                total_singles,
                total_doubles,
                total_triples,
                total_homeruns,
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                total_hbp,
                total_errors,
                total_sac_hit,
                total_sac_fly,
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
                total_pitches,
                total_pa,
                total_at_bats,
                total_outs,
                total_hits,
                total_bb,
                total_k,
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                popup_rate,
                hard_hit_rate,
                medium_hit_rate,
                soft_hit_rate,
                barrel_rate,
                avg_launch_speed,
                avg_launch_angle,
                avg_hit_distance,
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
                total_swings,
                total_swings_made_contact,
                total_called_strikes,
                total_swinging_strikes,
                total_inside_strike_zone,
                total_outside_strike_zone,
                total_swings_inside_zone,
                total_swings_outside_zone,
                total_contact_inside_zone,
                total_contact_outside_zone,
                total_balls_in_play,
                total_ground_balls,
                total_line_drives,
                total_fly_balls,
                total_popups,
                total_hard_hits,
                total_medium_hits,
                total_soft_hits,
                total_barrels,
                total_singles,
                total_doubles,
                total_triples,
                total_homeruns,
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                total_hbp,
                total_errors,
                total_sac_hit,
                total_sac_fly,
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
                total_pitches,
                total_pa,
                total_at_bats,
                total_outs,
                total_hits,
                total_bb,
                total_k,
                func.avg(db.PitchFx.start_speed).label("avg_speed"),
                func.avg(db.PitchFx.pfx_x).label("avg_pfx_x"),
                func.avg(db.PitchFx.pfx_z).label("avg_pfx_z"),
                func.avg(db.PitchFx.px).label("avg_px"),
                func.avg(db.PitchFx.pz).label("avg_pz"),
                func.avg(db.PitchFx.plate_time).label("avg_plate_time"),
                func.avg(db.PitchFx.extension).label("avg_extension"),
                func.avg(db.PitchFx.break_angle).label("avg_break_angle"),
                func.avg(db.PitchFx.break_length).label("avg_break_length"),
                func.avg(db.PitchFx.break_y).label("avg_break_y"),
                func.avg(db.PitchFx.spin_rate).label("avg_spin_rate"),
                func.avg(db.PitchFx.spin_direction).label("avg_spin_direction"),
                avg,
                obp,
                slg,
                ops,
                iso,
                fly_ball_rate,
                ground_ball_rate,
                line_drive_rate,
                popup_rate,
                hard_hit_rate,
                medium_hit_rate,
                soft_hit_rate,
                barrel_rate,
                avg_launch_speed,
                avg_launch_angle,
                avg_hit_distance,
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
                total_swings,
                total_swings_made_contact,
                total_called_strikes,
                total_swinging_strikes,
                total_inside_strike_zone,
                total_outside_strike_zone,
                total_swings_inside_zone,
                total_swings_outside_zone,
                total_contact_inside_zone,
                total_contact_outside_zone,
                total_balls_in_play,
                total_ground_balls,
                total_line_drives,
                total_fly_balls,
                total_popups,
                total_hard_hits,
                total_medium_hits,
                total_soft_hits,
                total_barrels,
                total_singles,
                total_doubles,
                total_triples,
                total_homeruns,
                func.sum(db.PitchFx.ab_result_ibb).label("total_ibb"),
                total_hbp,
                total_errors,
                total_sac_hit,
                total_sac_fly,
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
