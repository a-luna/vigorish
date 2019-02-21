"""Populate reference tables with data."""
from app.main.models.runners_on_base import RunnersOnBase


def populate_base_tables(session):
    """Populate reference tables with data."""
    print('\nPopulating base tables...')
    result = __add_runners_on_base(session)
    if not result['success']:
        return result
    session.commit()

    print('Complete!')
    return dict(success=True)

def __add_runners_on_base(session):
    try:
        rob0 = RunnersOnBase(
            runner_on_1b= False,
            runner_on_2b= False,
            runner_on_3b= False,
            scoring_position= False,
            base_open= True,
            dp_possible= False,
            notation= '---'
        )

        rob1 = RunnersOnBase(
            runner_on_1b= False,
            runner_on_2b= False,
            runner_on_3b= True,
            scoring_position= True,
            base_open= True,
            dp_possible= False,
            notation= '--3'
        )

        rob2 = RunnersOnBase(
            runner_on_1b= False,
            runner_on_2b= True,
            runner_on_3b= False,
            scoring_position= True,
            base_open= True,
            dp_possible= False,
            notation= '-2-'
        )

        rob3 = RunnersOnBase(
            runner_on_1b= False,
            runner_on_2b= True,
            runner_on_3b= True,
            scoring_position= True,
            base_open= True,
            dp_possible= False,
            notation= '-23'
        )

        rob4 = RunnersOnBase(
            runner_on_1b= True,
            runner_on_2b= False,
            runner_on_3b= False,
            scoring_position= False,
            base_open= True,
            dp_possible= True,
            notation= '1--'
        )

        rob5 = RunnersOnBase(
            runner_on_1b= True,
            runner_on_2b= False,
            runner_on_3b= True,
            scoring_position= True,
            base_open= True,
            dp_possible= True,
            notation= '1-3'
        )

        rob6 = RunnersOnBase(
            runner_on_1b= True,
            runner_on_2b= True,
            runner_on_3b= False,
            scoring_position= True,
            base_open= True,
            dp_possible= True,
            notation= '12-'
        )

        rob7 = RunnersOnBase(
            runner_on_1b= True,
            runner_on_2b= True,
            runner_on_3b= True,
            scoring_position= True,
            base_open= False,
            dp_possible= True,
            notation= '123'
        )

        session.add(rob0)
        session.add(rob1)
        session.add(rob2)
        session.add(rob3)
        session.add(rob4)
        session.add(rob5)
        session.add(rob6)
        session.add(rob7)
        return dict(success=True)
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        session.rollback()
        return dict(success=False, message=error)
