from app.main.data.setup.populate_base_tables import populate_base_tables
from app.main.data.setup.populate_seasons import populate_seasons
from app.main.data.setup.populate_players import populate_players
from app.main.data.setup.populate_teams import populate_teams

def initialize_database(session):
    result = populate_base_tables(session)
    if not result['success']:
        return result
    result = populate_seasons(session)
    if not result['success']:
        return result
    result = populate_players(session)
    if not result['success']:
        return result
    result = populate_teams(session)
    if not result['success']:
        return result
    return dict(success=True)