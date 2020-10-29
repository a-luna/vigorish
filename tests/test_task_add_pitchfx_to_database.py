import pytest

from tests.util import (
    COMBINED_DATA_GAME_DICT,
    update_scraped_bbref_games_for_date,
    update_scraped_boxscore,
    update_scraped_brooks_games_for_date,
    update_scraped_pitch_logs,
    update_scraped_pitchfx_logs,
)
from vigorish.config.database import get_total_number_of_rows, PitchFx
from vigorish.tasks.add_pitchfx_to_database import AddPitchFxToDatabase


@pytest.fixture(scope="module", autouse=True)
def create_test_data(db_session, scraped_data):
    """Initialize DB with data to verify test functions in test_cli module."""
    game_id_dict = COMBINED_DATA_GAME_DICT["NO_ERRORS"]
    game_date = game_id_dict["game_date"]
    bbref_game_id = game_id_dict["bbref_game_id"]
    bb_game_id = game_id_dict["bb_game_id"]
    apply_patch_list = game_id_dict["apply_patch_list"]
    update_scraped_bbref_games_for_date(db_session, scraped_data, game_date)
    update_scraped_brooks_games_for_date(db_session, scraped_data, game_date)
    update_scraped_boxscore(db_session, scraped_data, bbref_game_id)
    update_scraped_pitch_logs(db_session, scraped_data, game_date, bbref_game_id)
    update_scraped_pitchfx_logs(db_session, scraped_data, bb_game_id)
    scraped_data.combine_boxscore_and_pfx_data(bbref_game_id, apply_patch_list)
    db_session.commit()
    return True


def test_add_pitchfx_to_database(vig_app):
    db_session = vig_app["db_session"]
    total_rows = get_total_number_of_rows(db_session, PitchFx)
    assert total_rows == 0
    audit_report = vig_app["scraped_data"].get_audit_report()
    add_pfx_to_db = AddPitchFxToDatabase(vig_app)
    result = add_pfx_to_db.execute(audit_report, 2019)
    assert result.success
    total_rows = get_total_number_of_rows(db_session, PitchFx)
    assert total_rows == 299
    result = add_pfx_to_db.execute(audit_report)
    assert result.success
    total_rows = get_total_number_of_rows(db_session, PitchFx)
    assert total_rows == 299
