from sqlalchemy import case, cast, Float, func

import vigorish.database as db

avg_numerator = func.sum(db.BatStats.hits)
avg_denominator = func.sum(db.BatStats.at_bats)
avg_ = avg_numerator / cast(avg_denominator, Float)
avg = case([(avg_denominator > 0, avg_)], else_=0.0).label("avg")

obp_numerator = func.sum(db.BatStats.hits) + func.sum(db.BatStats.bases_on_balls) + func.sum(db.BatStats.hit_by_pitch)
obp_denominator = (
    func.sum(db.BatStats.at_bats)
    + func.sum(db.BatStats.bases_on_balls)
    + func.sum(db.BatStats.hit_by_pitch)
    + func.sum(db.BatStats.sac_fly)
)
obp_ = obp_numerator / cast(obp_denominator, Float)
obp = case([(obp_denominator > 0, obp_)], else_=0.0).label("obp")

extra_base_hits = func.sum(db.BatStats.doubles) + func.sum(db.BatStats.triples) + func.sum(db.BatStats.homeruns)
singles = func.sum(db.BatStats.hits) - extra_base_hits
slg_numerator = (
    singles
    + (func.sum(db.BatStats.doubles) * 2)
    + (func.sum(db.BatStats.triples) * 3)
    + (func.sum(db.BatStats.homeruns) * 4)
)
slg_denominator = func.sum(db.BatStats.at_bats)
slg_ = slg_numerator / cast(slg_denominator, Float)
slg = case([(slg_denominator > 0, slg_)], else_=0.0).label("slg")

ops = (obp + slg).label("ops")
iso = (slg - avg).label("iso")

bb_rate_numerator = func.sum(db.BatStats.bases_on_balls)
bb_rate_denominator = func.sum(db.BatStats.plate_appearances)
bb_rate_ = bb_rate_numerator / cast(bb_rate_denominator, Float)
bb_rate = case([(bb_rate_denominator > 0, bb_rate_)], else_=0.0).label("bb_rate")

k_rate_numerator = func.sum(db.BatStats.strikeouts)
k_rate_denominator = func.sum(db.BatStats.plate_appearances)
k_rate_ = k_rate_numerator / cast(k_rate_denominator, Float)
k_rate = case([(k_rate_denominator > 0, k_rate_)], else_=0.0).label("k_rate")

contact_rate_numerator = func.sum(db.BatStats.at_bats) - func.sum(db.BatStats.strikeouts)
contact_rate_denominator = func.sum(db.BatStats.at_bats)
contact_rate_ = contact_rate_numerator / cast(contact_rate_denominator, Float)
contact_rate = case([(contact_rate_denominator > 0, contact_rate_)], else_=0.0).label("contact_rate")
