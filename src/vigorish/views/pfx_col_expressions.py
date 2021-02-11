from sqlalchemy import and_, case, cast, Float, func

import vigorish.database as db

zone_rate = case(
    [
        (
            func.count(db.PitchFx.id) > 0,
            (func.sum(db.PitchFx.inside_strike_zone) / cast(func.count(db.PitchFx.id), Float)),
        ),
    ],
    else_=0.0,
).label("zone_rate")

called_strike_rate = case(
    [
        (
            func.count(db.PitchFx.id) > 0,
            (func.sum(db.PitchFx.called_strike) / cast(func.count(db.PitchFx.id), Float)),
        ),
    ],
    else_=0.0,
).label("called_strike_rate")

swinging_strike_rate = case(
    [
        (
            func.count(db.PitchFx.id) > 0,
            (func.sum(db.PitchFx.swinging_strike) / cast(func.count(db.PitchFx.id), Float)),
        ),
    ],
    else_=0.0,
).label("swinging_strike_rate")

whiff_rate = case(
    [
        (
            func.sum(db.PitchFx.batter_did_swing) > 0,
            (func.sum(db.PitchFx.swinging_strike) / cast(func.sum(db.PitchFx.batter_did_swing), Float)),
        ),
    ],
    else_=0.0,
).label("whiff_rate")

csw_rate = case(
    [
        (
            func.count(db.PitchFx.id) > 0,
            (
                (func.sum(db.PitchFx.swinging_strike) + func.sum(db.PitchFx.called_strike))
                / cast(func.count(db.PitchFx.id), Float)
            ),
        ),
    ],
    else_=0.0,
).label("csw_rate")

o_swing_rate = case(
    [
        (
            func.sum(db.PitchFx.outside_strike_zone) > 0,
            (func.sum(db.PitchFx.swing_outside_zone) / cast(func.sum(db.PitchFx.outside_strike_zone), Float)),
        ),
    ],
    else_=0.0,
).label("o_swing_rate")

z_swing_rate = case(
    [
        (
            func.sum(db.PitchFx.inside_strike_zone) > 0,
            (func.sum(db.PitchFx.swing_inside_zone) / cast(func.sum(db.PitchFx.inside_strike_zone), Float)),
        ),
    ],
    else_=0.0,
).label("z_swing_rate")

swing_rate = case(
    [
        (
            func.count(db.PitchFx.id) > 0,
            (func.sum(db.PitchFx.batter_did_swing) / cast(func.count(db.PitchFx.id), Float)),
        ),
    ],
    else_=0.0,
).label("swing_rate")

o_contact_rate = case(
    [
        (
            func.sum(db.PitchFx.swing_outside_zone) > 0,
            (func.sum(db.PitchFx.contact_outside_zone) / cast(func.sum(db.PitchFx.swing_outside_zone), Float)),
        ),
    ],
    else_=0.0,
).label("o_contact_rate")

z_contact_rate = case(
    [
        (
            func.sum(db.PitchFx.swing_inside_zone) > 0,
            (func.sum(db.PitchFx.contact_inside_zone) / cast(func.sum(db.PitchFx.swing_inside_zone), Float)),
        ),
    ],
    else_=0.0,
).label("z_contact_rate")

contact_rate = case(
    [
        (
            func.count(db.PitchFx.id) > 0,
            (func.sum(db.PitchFx.batter_made_contact) / cast(func.count(db.PitchFx.id), Float)),
        ),
    ],
    else_=0.0,
).label("contact_rate")

custom_score = case(
    [
        (
            and_(func.count(db.PitchFx.id) > 0, func.sum(db.PitchFx.outside_strike_zone) > 0),
            (
                (func.sum(db.PitchFx.inside_strike_zone) / cast(func.count(db.PitchFx.id), Float))
                + (func.sum(db.PitchFx.swinging_strike) / cast(func.count(db.PitchFx.id), Float))
                + (func.sum(db.PitchFx.swing_outside_zone) / cast(func.sum(db.PitchFx.outside_strike_zone), Float))
            ),
        ),
    ],
    else_=0.0,
).label("custom_score")

money_pitch = case(
    [
        (
            and_(func.count(db.PitchFx.id) > 0, func.sum(db.PitchFx.outside_strike_zone) > 0),
            (
                and_(
                    (func.sum(db.PitchFx.inside_strike_zone) / cast(func.count(db.PitchFx.id), Float)) > 0.4,
                    (func.sum(db.PitchFx.swing_outside_zone) / cast(func.sum(db.PitchFx.outside_strike_zone), Float))
                    > 0.4,
                    (func.sum(db.PitchFx.swinging_strike) / cast(func.count(db.PitchFx.id), Float)) > 0.15,
                )
            ),
        ),
    ],
    else_=False,
).label("money_pitch")

ground_ball_rate = case(
    [
        (
            func.sum(db.PitchFx.is_batted_ball) > 0,
            (func.sum(db.PitchFx.is_ground_ball) / cast(func.sum(db.PitchFx.is_batted_ball), Float)),
        ),
    ],
    else_=0.0,
).label("ground_ball_rate")

fly_ball_rate = case(
    [
        (
            func.sum(db.PitchFx.is_batted_ball) > 0,
            (func.sum(db.PitchFx.is_fly_ball) / cast(func.sum(db.PitchFx.is_batted_ball), Float)),
        ),
    ],
    else_=0.0,
).label("fly_ball_rate")

line_drive_rate = case(
    [
        (
            func.sum(db.PitchFx.is_batted_ball) > 0,
            (func.sum(db.PitchFx.is_line_drive) / cast(func.sum(db.PitchFx.is_batted_ball), Float)),
        ),
    ],
    else_=0.0,
).label("line_drive_rate")

pop_up_rate = case(
    [
        (
            func.sum(db.PitchFx.is_batted_ball) > 0,
            (func.sum(db.PitchFx.is_pop_up) / cast(func.sum(db.PitchFx.is_batted_ball), Float)),
        ),
    ],
    else_=0.0,
).label("pop_up_rate")
