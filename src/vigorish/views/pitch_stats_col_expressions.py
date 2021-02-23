from sqlalchemy import case, cast, Float, func, Integer, String

import vigorish.database as db

innings_whole_ = func.sum(db.PitchStats.total_outs) / 3
innings_whole = cast(cast(innings_whole_, Integer), String)
innings_remainder_ = func.sum(db.PitchStats.total_outs) % 3
innings_remainder = cast(cast(innings_remainder_, Integer), String)
innings_pitched_ = innings_whole + "." + innings_remainder
innings_pitched = cast(innings_pitched_, Float).label("innings_pitched")

era_numerator = func.sum(db.PitchStats.earned_runs) * 27
era_denominator = func.sum(db.PitchStats.total_outs)
era_ = era_numerator / cast(era_denominator, Float)
era = case([(era_denominator > 0, era_)], else_=0.0).label("era")

whip_numerator = (func.sum(db.PitchStats.bases_on_balls) + func.sum(db.PitchStats.hits)) * 3
whip_denominator = func.sum(db.PitchStats.total_outs)
whip_ = whip_numerator / cast(whip_denominator, Float)
whip = case([(whip_denominator > 0, whip_)], else_=0.0).label("whip")

k9_numerator = func.sum(db.PitchStats.strikeouts) * 27
k9_denominator = func.sum(db.PitchStats.total_outs)
k_per_nine_ = k9_numerator / cast(k9_denominator, Float)
k_per_nine = case([(k9_denominator > 0, k_per_nine_)], else_=0.0).label("k_per_nine")

bb9_numerator = func.sum(db.PitchStats.bases_on_balls) * 27
bb9_denominator = func.sum(db.PitchStats.total_outs)
bb_per_nine_ = bb9_numerator / cast(bb9_denominator, Float)
bb_per_nine = case([(bb9_denominator > 0, bb_per_nine_)], else_=0.0).label("bb_per_nine")

hr9_numerator = func.sum(db.PitchStats.homeruns) * 27
hr9_denominator = func.sum(db.PitchStats.total_outs)
hr9_per_nine_ = hr9_numerator / cast(hr9_denominator, Float)
hr_per_nine = case([(hr9_denominator > 0, hr9_per_nine_)], else_=0.0).label("hr_per_nine")

k_per_bb_numerator = func.sum(db.PitchStats.strikeouts)
k_per_bb_denominator = func.sum(db.PitchStats.bases_on_balls)
k_per_bb_ = k_per_bb_numerator / cast(k_per_bb_denominator, Float)
k_per_bb = case([(k_per_bb_denominator > 0, k_per_bb_)], else_=0.0).label("k_per_bb")

k_rate_numerator = func.sum(db.PitchStats.strikeouts)
k_rate_denominator = func.sum(db.PitchStats.batters_faced)
k_rate_ = k_rate_numerator / cast(k_rate_denominator, Float)
k_rate = case([(k_rate_denominator > 0, k_rate_)], else_=0.0).label("k_rate")

bb_rate_numerator = func.sum(db.PitchStats.bases_on_balls)
bb_rate_denominator = func.sum(db.PitchStats.batters_faced)
bb_rate_ = bb_rate_numerator / cast(bb_rate_denominator, Float)
bb_rate = case([(bb_rate_denominator > 0, bb_rate_)], else_=0.0).label("bb_rate")

k_minus_bb = (k_rate_ - bb_rate_).label("k_minus_bb")

hr_per_fb_numerator = func.sum(db.PitchStats.homeruns)
hr_per_fb_denominator = func.sum(db.PitchStats.fly_balls)
hr_per_fb_ = hr_per_fb_numerator / cast(hr_per_fb_denominator, Float)
hr_per_fb = case([(hr_per_fb_denominator > 0, hr_per_fb_)], else_=0.0).label("hr_per_fb")
