"""Populate reference tables with data."""
from vigorish.models.runners_on_base import RunnersOnBase
from vigorish.util.result import Result


def populate_base_tables(session):
    """Populate reference tables with data."""
    result = __add_runners_on_base(session)
    if result.failure:
        return result
    session.commit()
    return Result.Ok()


def __add_runners_on_base(session):
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
        session.add_all(runners_on_base)
        return Result.Ok()
    except Exception as e:
        error = "Error: {error}".format(error=repr(e))
        session.rollback()
        return Result.Fail(error)
