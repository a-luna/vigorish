"""Delete specific table data or all data in database."""
from app.main.models.player import Player
from app.main.models.player_id import PlayerId
from app.main.models.runners_on_base import RunnersOnBase
from app.main.models.season import Season
from app.main.models.team import Team

def delete_all_data(session):
    """Delete all rows in all tables."""
    print('\nDeleting existing mlb player data...')
    result = __delete_playerids(session)
    if not result['success']:
        return result
    result = __delete_players(session)
    if not result['success']:
        return result

    print('Complete!\nDeleting existing mlb team data...')
    result = __delete_teams(session)
    if not result['success']:
        return result

    print('Complete!\nDeleting existing mlb season data...')
    result = __delete_mlb_seasons(session)
    if not result['success']:
        return result

    print('Complete!\nDeleting existing data in base tables...')
    result = __delete_base_table_data(session)
    if not result['success']:
        return result

    try:
        session.commit()
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        session.rollback()
        return dict(success=False, message=error)

    print('Complete!')
    return dict(success=True)

def __delete_playerids(session):
    try:
        session.query(PlayerId).delete()
        return dict(success=True)
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        session.rollback()
        return dict(success=False, message=error)

def __delete_players(session):
    try:
        session.query(Player).delete()
        return dict(success=True)
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        session.rollback()
        return dict(success=False, message=error)

def __delete_teams(session):
    try:
        session.query(Team).delete()
        return dict(success=True)
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        session.rollback()
        return dict(success=False, message=error)

def __delete_mlb_seasons(session):
    try:
        session.query(Season).delete()
        return dict(success=True)
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        session.rollback()
        return dict(success=False, message=error)

def __delete_base_table_data(session):
    result = __delete_runners_on_base(session)
    if not result['success']:
        return result
    return dict(success=True)

def __delete_runners_on_base(session):
    try:
        session.query(RunnersOnBase).delete()
        return dict(success=True)
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        session.rollback()
        return dict(success=False, message=error)
