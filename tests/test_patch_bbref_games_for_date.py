from tests.util import (
    parse_brooks_games_for_date_from_html,
    PATCH_BR_DAILY_GAME_DATE,
    update_scraped_bbref_games_for_date,
)
from vigorish.database import GameScrapeStatus


def test_patch_bbref_games_for_date(db_session, scraped_data):
    invalid_game_id = "MIL201709150"
    patched_game_id = "MIA201709150"
    update_scraped_bbref_games_for_date(db_session, scraped_data, PATCH_BR_DAILY_GAME_DATE)

    games_for_date_no_patch = parse_brooks_games_for_date_from_html(
        db_session, scraped_data, PATCH_BR_DAILY_GAME_DATE, apply_patch_list=False
    )
    assert games_for_date_no_patch
    assert games_for_date_no_patch.game_count == 14
    bbref_game_ids_no_patch = [game_info.bbref_game_id for game_info in games_for_date_no_patch.games]
    assert invalid_game_id not in bbref_game_ids_no_patch
    assert patched_game_id not in bbref_game_ids_no_patch
    invalid_game_id_status = GameScrapeStatus.find_by_bbref_game_id(db_session, invalid_game_id)
    assert invalid_game_id_status and invalid_game_id_status.bbref_game_id == invalid_game_id
    patched_game_id_status = GameScrapeStatus.find_by_bbref_game_id(db_session, patched_game_id)
    assert not patched_game_id_status

    games_for_date_patched = parse_brooks_games_for_date_from_html(
        db_session, scraped_data, PATCH_BR_DAILY_GAME_DATE, apply_patch_list=True
    )
    assert games_for_date_patched
    assert games_for_date_patched.game_count == 15
    bbref_game_ids_patched = [game_info.bbref_game_id for game_info in games_for_date_patched.games]
    assert invalid_game_id not in bbref_game_ids_patched
    assert patched_game_id in bbref_game_ids_patched
    invalid_game_id_status = GameScrapeStatus.find_by_bbref_game_id(db_session, invalid_game_id)
    assert not invalid_game_id_status
    patched_game_id_status = GameScrapeStatus.find_by_bbref_game_id(db_session, patched_game_id)
    assert patched_game_id_status and patched_game_id_status.bbref_game_id == patched_game_id
