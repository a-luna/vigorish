from app.main.setup.create_relationships import create_relationships
from app.main.setup.populate_base_tables import populate_base_tables
from app.main.setup.populate_seasons import populate_seasons
from app.main.setup.populate_status_tables import populate_status_tables
from app.main.setup.populate_players import populate_players
from app.main.setup.populate_teams import populate_teams

def initialize_database(session):
    result = populate_base_tables(session)
    if not result['success']:
        return result
    result = populate_seasons(session)
    if not result['success']:
        return result
    result = populate_status_tables(session)
    if not result['success']:
        return result
    result = populate_players(session)
    if not result['success']:
        return result
    result = populate_teams(session)
    if not result['success']:
        return result
    result = create_relationships(session)
    if not result['success']:
        return result

    return dict(success=True)