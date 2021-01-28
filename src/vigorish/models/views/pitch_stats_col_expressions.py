from sqlalchemy import case, cast, Float, func, Integer, String

from vigorish.database import PitchStats

innings_pitched = cast(
    cast(cast(func.sum(PitchStats.total_outs) / 3, Integer), String)
    + "."
    + cast(cast(func.sum(PitchStats.total_outs) % 3, Integer), String),
    Float,
).label("innings_pitched")

era = case(
    [
        (
            func.sum(PitchStats.total_outs) > 0,
            ((func.sum(PitchStats.earned_runs) * 27) / cast(func.sum(PitchStats.total_outs), Float)),
        ),
    ],
    else_=0.0,
).label("era")

whip = case(
    [
        (
            func.sum(PitchStats.total_outs) > 0,
            (
                ((func.sum(PitchStats.bases_on_balls) + func.sum(PitchStats.hits)) * 3)
                / cast(func.sum(PitchStats.total_outs), Float)
            ),
        ),
    ],
    else_=0.0,
).label("whip")

k_per_nine = case(
    [
        (
            func.sum(PitchStats.total_outs) > 0,
            ((func.sum(PitchStats.strikeouts) * 27) / cast(func.sum(PitchStats.total_outs), Float)),
        ),
    ],
    else_=0.0,
).label("k_per_nine")

bb_per_nine = case(
    [
        (
            func.sum(PitchStats.total_outs) > 0,
            ((func.sum(PitchStats.bases_on_balls) * 27) / cast(func.sum(PitchStats.total_outs), Float)),
        ),
    ],
    else_=0.0,
).label("bb_per_nine")

hr_per_nine = case(
    [
        (
            func.sum(PitchStats.total_outs) > 0,
            ((func.sum(PitchStats.homeruns) * 27) / cast(func.sum(PitchStats.total_outs), Float)),
        ),
    ],
    else_=0.0,
).label("hr_per_nine")

k_per_bb = case(
    [
        (
            func.sum(PitchStats.bases_on_balls) > 0,
            (func.sum(PitchStats.strikeouts) / cast(func.sum(PitchStats.bases_on_balls), Float)),
        ),
    ],
    else_=0.0,
).label("k_per_bb")

k_rate = case(
    [
        (
            func.sum(PitchStats.batters_faced) > 0,
            (func.sum(PitchStats.strikeouts) / cast(func.sum(PitchStats.batters_faced), Float)),
        ),
    ],
    else_=0.0,
).label("k_rate")

bb_rate = case(
    [
        (
            func.sum(PitchStats.batters_faced) > 0,
            (func.sum(PitchStats.bases_on_balls) / cast(func.sum(PitchStats.batters_faced), Float)),
        ),
    ],
    else_=0.0,
).label("bb_rate")

k_minus_bb = (k_rate - bb_rate).label("k_minus_bb")

hr_per_fb = case(
    [
        (
            func.sum(PitchStats.fly_balls) > 0,
            (func.sum(PitchStats.homeruns) / cast(func.sum(PitchStats.fly_balls), Float)),
        ),
    ],
    else_=0.0,
).label("hr_per_fb")
