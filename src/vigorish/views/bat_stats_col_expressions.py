from sqlalchemy import case, cast, Float, func

import vigorish.database as db

avg = case(
    [
        (
            func.sum(db.BatStats.at_bats) > 0,
            (func.sum(db.BatStats.hits) / cast(func.sum(db.BatStats.at_bats), Float)),
        ),
    ],
    else_=0.0,
).label("avg")

obp_numerator = func.sum(db.BatStats.hits) + func.sum(db.BatStats.bases_on_balls) + func.sum(db.BatStats.hit_by_pitch)
obp_denominator = (
    func.sum(db.BatStats.at_bats)
    + func.sum(db.BatStats.bases_on_balls)
    + func.sum(db.BatStats.hit_by_pitch)
    + func.sum(db.BatStats.sac_fly)
)
obp = case(
    [
        (
            obp_denominator > 0,
            (obp_numerator / cast(obp_denominator, Float)),
        ),
    ],
    else_=0.0,
).label("obp")

singles = func.sum(db.BatStats.hits) - (
    func.sum(db.BatStats.doubles) + func.sum(db.BatStats.triples) + func.sum(db.BatStats.homeruns)
)
slg_numerator = (
    singles
    + (func.sum(db.BatStats.doubles) * 2)
    + (func.sum(db.BatStats.triples) * 3)
    + (func.sum(db.BatStats.homeruns) * 4)
)
slg = case(
    [
        (
            func.sum(db.BatStats.at_bats) > 0,
            (slg_numerator / cast(func.sum(db.BatStats.at_bats), Float)),
        ),
    ],
    else_=0.0,
).label("slg")

ops = (obp + slg).label("ops")
iso = (slg - avg).label("iso")

bb_rate = case(
    [
        (
            func.sum(db.BatStats.plate_appearances) > 0,
            (func.sum(db.BatStats.bases_on_balls) / cast(func.sum(db.BatStats.plate_appearances), Float)),
        ),
    ],
    else_=0.0,
).label("bb_rate")

k_rate = case(
    [
        (
            func.sum(db.BatStats.plate_appearances) > 0,
            (func.sum(db.BatStats.strikeouts) / cast(func.sum(db.BatStats.plate_appearances), Float)),
        ),
    ],
    else_=0.0,
).label("k_rate")
