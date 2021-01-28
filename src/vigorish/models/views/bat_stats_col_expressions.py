from sqlalchemy import case, cast, Float, func

from vigorish.database import BatStats

avg = case(
    [
        (
            func.sum(BatStats.at_bats) > 0,
            (func.sum(BatStats.hits) / cast(func.sum(BatStats.at_bats), Float)),
        ),
    ],
    else_=0.0,
).label("avg")

obp_numerator = func.sum(BatStats.hits) + func.sum(BatStats.bases_on_balls) + func.sum(BatStats.hit_by_pitch)
obp_denominator = (
    func.sum(BatStats.at_bats)
    + func.sum(BatStats.bases_on_balls)
    + func.sum(BatStats.hit_by_pitch)
    + func.sum(BatStats.sac_fly)
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

singles = func.sum(BatStats.hits) - (
    func.sum(BatStats.doubles) + func.sum(BatStats.triples) + func.sum(BatStats.homeruns)
)
slg_numerator = (
    singles + (func.sum(BatStats.doubles) * 2) + (func.sum(BatStats.triples) * 3) + (func.sum(BatStats.homeruns) * 4)
)
slg = case(
    [
        (
            func.sum(BatStats.at_bats) > 0,
            (slg_numerator / cast(func.sum(BatStats.at_bats), Float)),
        ),
    ],
    else_=0.0,
).label("slg")

ops = (obp + slg).label("ops")
iso = (slg - avg).label("iso")

bb_rate = case(
    [
        (
            func.sum(BatStats.plate_appearances) > 0,
            (func.sum(BatStats.bases_on_balls) / cast(func.sum(BatStats.plate_appearances), Float)),
        ),
    ],
    else_=0.0,
).label("bb_rate")

k_rate = case(
    [
        (
            func.sum(BatStats.plate_appearances) > 0,
            (func.sum(BatStats.strikeouts) / cast(func.sum(BatStats.plate_appearances), Float)),
        ),
    ],
    else_=0.0,
).label("k_rate")
