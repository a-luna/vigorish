from vigorish.setup.create_relationships import create_relationships
from vigorish.setup.populate_base_tables import populate_base_tables
from vigorish.setup.populate_seasons import populate_seasons
from vigorish.setup.populate_status_tables import populate_status_tables
from vigorish.setup.populate_players import populate_players
from vigorish.setup.populate_teams import populate_teams


def populate_tables(session):
    return (
        populate_base_tables(session)
        .on_success(populate_seasons, session)
        .on_success(populate_status_tables, session)
        .on_success(populate_players, session)
        .on_success(populate_teams, session)
        .on_success(create_relationships, session)
    )
