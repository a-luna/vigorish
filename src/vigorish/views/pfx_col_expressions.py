from sqlalchemy import and_, case, cast, Float, func

import vigorish.database as db

total_pitches = func.count(db.PitchFx.id).label("total_pitches")
total_inside_strike_zone = func.sum(db.PitchFx.inside_strike_zone).label("total_inside_strike_zone")
total_outside_strike_zone = func.sum(db.PitchFx.outside_strike_zone).label("total_outside_strike_zone")

total_called_strikes = func.sum(db.PitchFx.called_strike).label("total_called_strikes")
total_swinging_strikes = func.sum(db.PitchFx.swinging_strike).label("total_swinging_strikes")

total_swings = func.sum(db.PitchFx.batter_did_swing).label("total_swings")
total_swings_inside_zone = func.sum(db.PitchFx.swing_inside_zone).label("total_swings_inside_zone")
total_swings_outside_zone = func.sum(db.PitchFx.swing_outside_zone).label("total_swings_outside_zone")

total_swings_made_contact = func.sum(db.PitchFx.batter_made_contact).label("total_swings_made_contact")
total_contact_inside_zone = func.sum(db.PitchFx.contact_inside_zone).label("total_contact_inside_zone")
total_contact_outside_zone = func.sum(db.PitchFx.contact_outside_zone).label("total_contact_outside_zone")

total_balls_in_play = func.sum(db.PitchFx.is_in_play).label("total_balls_in_play")
total_ground_balls = func.sum(db.PitchFx.is_ground_ball).label("total_ground_balls")
total_line_drives = func.sum(db.PitchFx.is_line_drive).label("total_line_drives")
total_fly_balls = func.sum(db.PitchFx.is_fly_ball).label("total_fly_balls")
total_popups = func.sum(db.PitchFx.is_popup).label("total_popups")

total_pa = func.sum(db.PitchFx.is_final_pitch_of_ab).label("total_pa")
total_hits = func.sum(db.PitchFx.ab_result_hit).label("total_hits")
total_outs = func.sum(db.PitchFx.ab_result_out).label("total_outs")
total_k = func.sum(db.PitchFx.ab_result_k).label("total_k")
total_bb = func.sum(db.PitchFx.ab_result_bb).label("total_bb")
total_hbp = func.sum(db.PitchFx.ab_result_hbp).label("total_hbp")
total_sac_hit = func.sum(db.PitchFx.ab_result_sac_hit).label("total_sac_hit")
total_sac_fly = func.sum(db.PitchFx.ab_result_sac_fly).label("total_sac_fly")
total_errors = func.sum(db.PitchFx.ab_result_error).label("total_errors")
total_at_bats = (total_hits + total_outs + total_errors - total_sac_hit - total_sac_fly).label("total_at_bats")

total_singles = func.sum(db.PitchFx.ab_result_single).label("total_singles")
total_doubles = func.sum(db.PitchFx.ab_result_double).label("total_doubles")
total_triples = func.sum(db.PitchFx.ab_result_triple).label("total_triples")
total_homeruns = func.sum(db.PitchFx.ab_result_homerun).label("total_homeruns")

total_hard_hits = func.sum(db.PitchFx.is_hard_hit).label("total_hard_hits")
total_medium_hits = func.sum(db.PitchFx.is_medium_hit).label("total_medium_hits")
total_soft_hits = func.sum(db.PitchFx.is_soft_hit).label("total_soft_hits")
total_barrels = func.sum(db.PitchFx.is_barreled).label("total_barrels")

bad_whiff = case([(and_(db.PitchFx.swinging_strike == 1, db.PitchFx.outside_strike_zone == 1), 1)], else_=0).label(
    "bad_whiff"
)
total_bad_whiffs = func.sum(bad_whiff).label("total_bad_whiffs")
bad_whiff_rate = total_bad_whiffs / cast(total_swings, Float)
bad_whiff_rate = case([(total_swings > 0, bad_whiff_rate)], else_=0.0).label("bad_whiff_rate")

zone_rate = total_inside_strike_zone / cast(total_pitches, Float)
zone_rate = case([(total_pitches > 0, zone_rate)], else_=0.0).label("zone_rate")

called_strike_rate = total_called_strikes / cast(total_pitches, Float)
called_strike_rate = case([(total_pitches > 0, called_strike_rate)], else_=0.0).label("called_strike_rate")

swinging_strike_rate = total_swinging_strikes / cast(total_pitches, Float)
swinging_strike_rate = case([(total_pitches > 0, swinging_strike_rate)], else_=0.0).label("swinging_strike_rate")

whiff_rate = total_swinging_strikes / cast(total_swings, Float)
whiff_rate = case([(total_swings > 0, whiff_rate)], else_=0.0).label("whiff_rate")

csw_rate = (total_swinging_strikes + total_called_strikes) / cast(total_pitches, Float)
csw_rate = case([(total_pitches > 0, csw_rate)], else_=0.0).label("csw_rate")

o_swing_rate_ = total_swings_outside_zone / cast(total_outside_strike_zone, Float)
o_swing_rate = case([(total_outside_strike_zone > 0, o_swing_rate_)], else_=0.0).label("o_swing_rate")

z_swing_rate = total_swings_inside_zone / cast(total_inside_strike_zone, Float)
z_swing_rate = case([(total_inside_strike_zone > 0, z_swing_rate)], else_=0.0).label("z_swing_rate")

swing_rate = total_swings / cast(total_pitches, Float)
swing_rate = case([(total_pitches > 0, swing_rate)], else_=0.0).label("swing_rate")

o_contact_rate = total_contact_outside_zone / cast(total_swings_outside_zone, Float)
o_contact_rate = case([(total_swings_outside_zone > 0, o_contact_rate)], else_=0.0).label("o_contact_rate")

z_contact_rate = total_contact_inside_zone / cast(total_swings_inside_zone, Float)
z_contact_rate = case([(total_swings_inside_zone > 0, z_contact_rate)], else_=0.0).label("z_contact_rate")

contact_rate = total_swings_made_contact / cast(total_swings, Float)
contact_rate = case([(total_swings > 0, contact_rate)], else_=0.0).label("contact_rate")

ground_ball_rate = total_ground_balls / cast(total_balls_in_play, Float)
ground_ball_rate = case([(total_balls_in_play > 0, ground_ball_rate)], else_=0.0).label("ground_ball_rate")

fly_ball_rate = total_fly_balls / cast(total_balls_in_play, Float)
fly_ball_rate = case([(total_balls_in_play > 0, fly_ball_rate)], else_=0.0).label("fly_ball_rate")

line_drive_rate = total_line_drives / cast(total_balls_in_play, Float)
line_drive_rate = case([(total_balls_in_play > 0, line_drive_rate)], else_=0.0).label("line_drive_rate")

popup_rate = total_popups / cast(total_balls_in_play, Float)
popup_rate = case([(total_balls_in_play > 0, popup_rate)], else_=0.0).label("popup_rate")

avg = total_hits / cast(total_at_bats, Float)
avg = case([(total_at_bats > 0, avg)], else_=0.0).label("avg")

obp_numerator = total_hits + total_bb + total_hbp
obp_denominator = total_at_bats + total_bb + total_hbp + total_sac_fly
obp = obp_numerator / cast(obp_denominator, Float)
obp = case([(obp_denominator > 0, obp)], else_=0.0).label("obp")

slg_numerator = total_singles + (total_doubles * 2) + (total_triples * 3) + (total_homeruns * 4)
slg = slg_numerator / cast(total_at_bats, Float)
slg = case([(total_at_bats > 0, slg)], else_=0.0).label("slg")

ops = (obp + slg).label("ops")
iso = (slg - avg).label("iso")

bb_rate = total_bb / cast(total_pa, Float)
bb_rate = case([(total_pa > 0, bb_rate)], else_=0.0).label("bb_rate")

k_rate = total_k / cast(total_pa, Float)
k_rate = case([(total_pa > 0, k_rate)], else_=0.0).label("k_rate")

hr_per_fb = total_homeruns / cast(total_fly_balls, Float)
hr_per_fb = case([(total_fly_balls > 0, hr_per_fb)], else_=0.0).label("hr_per_fb")

avg_launch_speed = func.sum(db.PitchFx.launch_speed) / cast(total_balls_in_play, Float)
avg_launch_speed = case([(total_balls_in_play > 0, avg_launch_speed)], else_=0.0).label("avg_launch_speed")

max_launch_speed = func.max(db.PitchFx.launch_speed).label("max_launch_speed")

avg_launch_angle = func.sum(db.PitchFx.launch_angle) / cast(total_balls_in_play, Float)
avg_launch_angle = case([(total_balls_in_play > 0, avg_launch_angle)], else_=0.0).label("avg_launch_angle")

avg_hit_distance = func.sum(db.PitchFx.total_distance) / cast(total_balls_in_play, Float)
avg_hit_distance = case([(total_balls_in_play > 0, avg_hit_distance)], else_=0.0).label("avg_hit_distance")

hard_hit_rate = total_hard_hits / cast(total_balls_in_play, Float)
hard_hit_rate = case([(total_balls_in_play > 0, hard_hit_rate)], else_=0.0).label("hard_hit_rate")

medium_hit_rate = total_medium_hits / cast(total_balls_in_play, Float)
medium_hit_rate = case([(total_balls_in_play > 0, medium_hit_rate)], else_=0.0).label("medium_hit_rate")

soft_hit_rate = total_soft_hits / cast(total_balls_in_play, Float)
soft_hit_rate = case([(total_balls_in_play > 0, soft_hit_rate)], else_=0.0).label("soft_hit_rate")

barrel_rate = total_barrels / cast(total_balls_in_play, Float)
barrel_rate = case([(total_balls_in_play > 0, barrel_rate)], else_=0.0).label("barrel_rate")
