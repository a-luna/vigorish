from vigorish.setup.create_relationships import create_relationships
from vigorish.setup.populate_base_tables import populate_base_tables
from vigorish.setup.populate_pitch_type_perc import populate_pitch_type_perc
from vigorish.setup.populate_players import populate_players
from vigorish.setup.populate_seasons import populate_seasons
from vigorish.setup.populate_status_tables import populate_status_tables
from vigorish.setup.populate_teams import populate_teams


def populate_tables(app, csv_folder, json_folder):
    db_session = app.db_session
    result = (
        populate_base_tables(db_session)
        .on_success(populate_seasons, db_session)
        .on_success(populate_status_tables, db_session)
        .on_success(populate_teams, db_session, csv_folder)
        .on_success(populate_players, app, csv_folder)
        .on_success(populate_pitch_type_perc, app, json_folder)
        .on_success(create_relationships, db_session)
    )
    if result.failure:
        db_session.rollback()
        return result
    db_session.commit()
    return result


def populate_tables_for_restore(app, csv_folder, json_folder):
    db_session = app.db_session
    result = (
        populate_base_tables(db_session)
        .on_success(populate_seasons, db_session)
        .on_success(populate_teams, db_session, csv_folder)
        .on_success(populate_players, app, csv_folder)
        .on_success(populate_pitch_type_perc, app, json_folder)
        .on_success(create_relationships, db_session)
    )
    if result.failure:
        db_session.rollback()
        return result
    db_session.commit()
    return result
