"""Populate reference tables with data."""
from vigorish.config.database import RunnersOnBase, TimeBetweenPitches
from vigorish.util.result import Result


def populate_base_tables(db_session):
    """Populate reference tables with data."""
    result = add_time_between_pitches(db_session)
    if result.failure:
        return result
    result = add_runners_on_base(db_session)
    if result.failure:
        return result
    db_session.commit()
    return Result.Ok()


def add_time_between_pitches(db_session):
    result = TimeBetweenPitches.from_calc_results(db_session, get_avg_pitch_times())
    return Result.Ok() if result else Result.Fail("failed to populate time_between_pitches")


def get_avg_pitch_times():
    return {
        "time_between_pitches": {
            "total": 16478993,
            "count": 692284,
            "avg": 23.8,
            "max": 48.0,
            "min": 3.0,
        },
        "time_between_at_bats": {
            "total": 7118617,
            "count": 165166,
            "avg": 43.1,
            "max": 79.0,
            "min": 28.0,
        },
        "time_between_innings": {
            "total": 8432869,
            "count": 52589,
            "avg": 160.4,
            "max": 306.0,
            "min": 131.0,
        },
    }


def add_runners_on_base(db_session):
    try:
        runners_on_base = [
            RunnersOnBase(
                runner_on_1b=False,
                runner_on_2b=False,
                runner_on_3b=False,
                scoring_position=False,
                base_open=True,
                dp_possible=False,
                notation="---",
            ),
            RunnersOnBase(
                runner_on_1b=False,
                runner_on_2b=False,
                runner_on_3b=True,
                scoring_position=True,
                base_open=True,
                dp_possible=False,
                notation="--3",
            ),
            RunnersOnBase(
                runner_on_1b=False,
                runner_on_2b=True,
                runner_on_3b=False,
                scoring_position=True,
                base_open=True,
                dp_possible=False,
                notation="-2-",
            ),
            RunnersOnBase(
                runner_on_1b=False,
                runner_on_2b=True,
                runner_on_3b=True,
                scoring_position=True,
                base_open=True,
                dp_possible=False,
                notation="-23",
            ),
            RunnersOnBase(
                runner_on_1b=True,
                runner_on_2b=False,
                runner_on_3b=False,
                scoring_position=False,
                base_open=True,
                dp_possible=True,
                notation="1--",
            ),
            RunnersOnBase(
                runner_on_1b=True,
                runner_on_2b=False,
                runner_on_3b=True,
                scoring_position=True,
                base_open=True,
                dp_possible=True,
                notation="1-3",
            ),
            RunnersOnBase(
                runner_on_1b=True,
                runner_on_2b=True,
                runner_on_3b=False,
                scoring_position=True,
                base_open=True,
                dp_possible=True,
                notation="12-",
            ),
            RunnersOnBase(
                runner_on_1b=True,
                runner_on_2b=True,
                runner_on_3b=True,
                scoring_position=True,
                base_open=False,
                dp_possible=True,
                notation="123",
            ),
        ]
        db_session.add_all(runners_on_base)
        return Result.Ok()
    except Exception as e:
        error = "Error: {error}".format(error=repr(e))
        db_session.rollback()
        return Result.Fail(error)
