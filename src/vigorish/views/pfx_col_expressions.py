from sqlalchemy import and_, case, cast, Float, func

import vigorish.database as db

zone_rate_numer = func.sum(db.PitchFx.inside_strike_zone)
zone_rate_denom = func.count(db.PitchFx.id)
zone_rate = zone_rate_numer / cast(zone_rate_denom, Float)
zone_rate = case([(zone_rate_denom > 0, zone_rate)], else_=0.0).label("zone_rate")

cs_rate_numer = func.sum(db.PitchFx.called_strike)
cs_rate_denom = func.count(db.PitchFx.id)
called_strike_rate = cs_rate_numer / cast(cs_rate_denom, Float)
called_strike_rate = case([(cs_rate_denom > 0, called_strike_rate)], else_=0.0).label("called_strike_rate")

ss_rate_numer = func.sum(db.PitchFx.swinging_strike)
ss_rate_denom = func.count(db.PitchFx.id)
swinging_strike_rate = ss_rate_numer / cast(ss_rate_denom, Float)
swinging_strike_rate = case([(ss_rate_denom > 0, swinging_strike_rate)], else_=0.0).label("swinging_strike_rate")

whiff_rate_numer = func.sum(db.PitchFx.swinging_strike)
whiff_rate_denom = func.sum(db.PitchFx.batter_did_swing)
whiff_rate = whiff_rate_numer / cast(whiff_rate_denom, Float)
whiff_rate = case([(whiff_rate_denom > 0, whiff_rate)], else_=0.0).label("whiff_rate")

csw_rate_numer = func.sum(db.PitchFx.swinging_strike) + func.sum(db.PitchFx.called_strike)
csw_rate_denom = func.count(db.PitchFx.id)
csw_rate = csw_rate_numer / cast(csw_rate_denom, Float)
csw_rate = case([(csw_rate_denom > 0, csw_rate)], else_=0.0).label("csw_rate")

os_rate_numer = func.sum(db.PitchFx.swing_outside_zone)
os_rate_denom = func.sum(db.PitchFx.outside_strike_zone)
o_swing_rate_ = os_rate_numer / cast(os_rate_denom, Float)
o_swing_rate = case([(os_rate_denom > 0, o_swing_rate_)], else_=0.0).label("o_swing_rate")

zs_rate_numer = func.sum(db.PitchFx.swing_inside_zone)
zs_rate_denom = func.sum(db.PitchFx.inside_strike_zone)
z_swing_rate = zs_rate_numer / cast(zs_rate_denom, Float)
z_swing_rate = case([(zs_rate_denom > 0, z_swing_rate)], else_=0.0).label("z_swing_rate")

sw_rate_numer = func.sum(db.PitchFx.batter_did_swing)
sw_rate_denom = func.count(db.PitchFx.id)
swing_rate = sw_rate_numer / cast(sw_rate_denom, Float)
swing_rate = case([(sw_rate_denom > 0, swing_rate)], else_=0.0).label("swing_rate")

oc_rate_numer = func.sum(db.PitchFx.contact_outside_zone)
oc_rate_denom = func.sum(db.PitchFx.swing_outside_zone)
o_contact_rate = oc_rate_numer / cast(oc_rate_denom, Float)
o_contact_rate = case([(oc_rate_denom > 0, o_contact_rate)], else_=0.0).label("o_contact_rate")

zc_rate_numer = func.sum(db.PitchFx.contact_inside_zone)
zc_rate_denom = func.sum(db.PitchFx.swing_inside_zone)
z_contact_rate = zc_rate_numer / cast(zc_rate_denom, Float)
z_contact_rate = case([(zc_rate_denom > 0, z_contact_rate)], else_=0.0).label("z_contact_rate")

contact_rate_numer = func.sum(db.PitchFx.batter_made_contact)
contact_rate_denom = func.count(db.PitchFx.id)
contact_rate = contact_rate_numer / cast(contact_rate_denom, Float)
contact_rate = case([(contact_rate_denom > 0, contact_rate)], else_=0.0).label("contact_rate")

custom_score = (swinging_strike_rate + o_swing_rate + zone_rate).label("custom_score")
money_pitch = and_(swinging_strike_rate > 0.15, o_swing_rate > 0.4, zone_rate > 0.4).label("money_pitch")

gb_rate_numer = func.sum(db.PitchFx.is_ground_ball)
gb_rate_denom = func.sum(db.PitchFx.is_batted_ball)
ground_ball_rate = gb_rate_numer / cast(gb_rate_denom, Float)
ground_ball_rate = case([(gb_rate_denom > 0, ground_ball_rate)], else_=0.0).label("ground_ball_rate")

fb_rate_numer = func.sum(db.PitchFx.is_fly_ball)
fb_rate_denom = func.sum(db.PitchFx.is_batted_ball)
fly_ball_rate = fb_rate_numer / cast(fb_rate_denom, Float)
fly_ball_rate = case([(fb_rate_denom > 0, fly_ball_rate)], else_=0.0).label("fly_ball_rate")

ld_rate_numer = func.sum(db.PitchFx.is_line_drive)
ld_rate_denom = func.sum(db.PitchFx.is_batted_ball)
line_drive_rate = ld_rate_numer / cast(ld_rate_denom, Float)
line_drive_rate = case([(ld_rate_denom > 0, line_drive_rate)], else_=0.0).label("line_drive_rate")

pu_rate_numer = func.sum(db.PitchFx.is_pop_up)
pu_rate_denom = func.sum(db.PitchFx.is_batted_ball)
pop_up_rate = pu_rate_numer / cast(pu_rate_denom, Float)
pop_up_rate = case([(pu_rate_denom > 0, pop_up_rate)], else_=0.0).label("pop_up_rate")
