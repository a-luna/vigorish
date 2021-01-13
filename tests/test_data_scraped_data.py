from datetime import datetime

import pytest

from tests.util import (
    update_scraped_bbref_games_for_date,
    update_scraped_boxscore,
    update_scraped_brooks_games_for_date,
)

GAME_DATE = datetime(2019, 7, 11)
BBREF_GAME_ID = "TEX201907110"


@pytest.fixture(scope="module", autouse=True)
def create_test_data(db_session, scraped_data):
    """Initialize DB with data to verify test functions in test_brooks_pitch_logs module."""
    update_scraped_bbref_games_for_date(db_session, scraped_data, GAME_DATE)
    update_scraped_brooks_games_for_date(db_session, scraped_data, GAME_DATE)
    update_scraped_boxscore(db_session, scraped_data, BBREF_GAME_ID)
    return True


def test_get_all_brooks_pitch_logs_for_date(vig_app):
    pitch_logs_for_date = vig_app.scraped_data.get_all_brooks_pitch_logs_for_date(GAME_DATE)
    assert len(pitch_logs_for_date) == 1
    pitch_logs_for_game = pitch_logs_for_date[0]
    assert pitch_logs_for_game.bbref_game_id == BBREF_GAME_ID
    pitch_app_ids = [plog.pitch_app_id for plog in pitch_logs_for_game.pitch_logs]
    assert "TEX201907110_455119" in pitch_app_ids
    assert "TEX201907110_458681" in pitch_app_ids
    assert "TEX201907110_600917" in pitch_app_ids
    assert "TEX201907110_605482" in pitch_app_ids
    assert "TEX201907110_606965" in pitch_app_ids
    assert "TEX201907110_657624" in pitch_app_ids
    assert "TEX201907110_664285" in pitch_app_ids
