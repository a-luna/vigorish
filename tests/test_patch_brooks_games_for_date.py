from tests.util import (
    INVALID_BR_GAME_ID1,
    INVALID_BR_GAME_ID2,
    PATCH_BB_DAILY_GAME_DATE,
    PATCHED_BR_GAME_ID1,
    update_scraped_bbref_games_for_date,
    update_scraped_brooks_games_for_date,
    update_scraped_pitch_logs,
)
from vigorish.database import GameScrapeStatus, PitchAppScrapeStatus
from vigorish.enums import DataSet
from vigorish.util.dt_format_strings import DATE_ONLY
from vigorish.patch.brooks_games_for_date import (
    BrooksGamesForDatePatchList,
    PatchBrooksGamesForDateBBRefGameID,
    PatchBrooksGamesForDateRemoveGame,
)


def test_patch_brooks_games_for_date(db_session, scraped_data):
    bbref_games_for_date_no_patch = update_scraped_bbref_games_for_date(
        db_session, scraped_data, PATCH_BB_DAILY_GAME_DATE
    )
    brooks_games_for_date_no_patch = update_scraped_brooks_games_for_date(
        db_session, scraped_data, PATCH_BB_DAILY_GAME_DATE
    )
    assert bbref_games_for_date_no_patch.game_count != brooks_games_for_date_no_patch.game_count

    game_ids_only_brooks_no_patch = list(
        set(brooks_games_for_date_no_patch.all_bbref_game_ids)
        - set(bbref_games_for_date_no_patch.all_bbref_game_ids)
    )
    assert len(game_ids_only_brooks_no_patch) == 2
    assert INVALID_BR_GAME_ID1 in game_ids_only_brooks_no_patch
    assert INVALID_BR_GAME_ID2 in game_ids_only_brooks_no_patch

    game_ids_only_bbref_no_patch = list(
        set(bbref_games_for_date_no_patch.all_bbref_game_ids)
        - set(brooks_games_for_date_no_patch.all_bbref_game_ids)
    )
    assert len(game_ids_only_bbref_no_patch) == 1
    assert PATCHED_BR_GAME_ID1 in game_ids_only_bbref_no_patch

    plogs_for_game_1_no_patch = update_scraped_pitch_logs(
        db_session=db_session,
        scraped_data=scraped_data,
        game_date=PATCH_BB_DAILY_GAME_DATE,
        bbref_game_id=INVALID_BR_GAME_ID1,
        apply_patch_list=False,
    )
    assert plogs_for_game_1_no_patch.bbref_game_id == INVALID_BR_GAME_ID1
    assert plogs_for_game_1_no_patch.pitch_log_count == 10
    assert all(plog.parsed_all_info for plog in plogs_for_game_1_no_patch.pitch_logs)

    pitch_apps_for_game_1_no_patch = (
        db_session.query(PitchAppScrapeStatus).filter_by(bbref_game_id=INVALID_BR_GAME_ID1).all()
    )
    assert len(pitch_apps_for_game_1_no_patch) == 10

    game_status_invalid_game_id = GameScrapeStatus.find_by_bbref_game_id(
        db_session, INVALID_BR_GAME_ID1
    )
    assert game_status_invalid_game_id

    plogs_for_game_2_no_patch = update_scraped_pitch_logs(
        db_session=db_session,
        scraped_data=scraped_data,
        game_date=PATCH_BB_DAILY_GAME_DATE,
        bbref_game_id=INVALID_BR_GAME_ID2,
        apply_patch_list=False,
    )
    assert plogs_for_game_2_no_patch.bbref_game_id == INVALID_BR_GAME_ID2
    assert plogs_for_game_2_no_patch.pitch_log_count == 8
    assert all(not plog.parsed_all_info for plog in plogs_for_game_2_no_patch.pitch_logs)

    pitch_apps_for_game_2_no_patch = (
        db_session.query(PitchAppScrapeStatus).filter_by(bbref_game_id=INVALID_BR_GAME_ID2).all()
    )
    assert len(pitch_apps_for_game_2_no_patch) == 8

    game_status_remove_game_id = GameScrapeStatus.find_by_bbref_game_id(
        db_session, INVALID_BR_GAME_ID2
    )
    assert game_status_remove_game_id

    patch_invalid_bbref_game_id = PatchBrooksGamesForDateBBRefGameID(
        game_date=PATCH_BB_DAILY_GAME_DATE.strftime(DATE_ONLY),
        invalid_bbref_game_id=INVALID_BR_GAME_ID1,
        valid_bbref_game_id=PATCHED_BR_GAME_ID1,
    )
    patch_remove_game_id = PatchBrooksGamesForDateRemoveGame(
        game_date=PATCH_BB_DAILY_GAME_DATE.strftime(DATE_ONLY),
        remove_bbref_game_id=INVALID_BR_GAME_ID2,
    )
    patch_list = BrooksGamesForDatePatchList(
        patch_list=[patch_invalid_bbref_game_id, patch_remove_game_id],
        url_id=PATCH_BB_DAILY_GAME_DATE.strftime(DATE_ONLY),
    )

    result = scraped_data.save_patch_list(DataSet.BROOKS_GAMES_FOR_DATE, patch_list)
    assert result.success
    saved_file_dict = result.value
    patch_list_filepath = saved_file_dict["local_filepath"]
    assert patch_list_filepath.exists()
    assert patch_list_filepath.name == "brooks_games_for_date_2017-05-26_PATCH_LIST.json"

    bbref_games_for_date_patched = scraped_data.get_bbref_games_for_date(PATCH_BB_DAILY_GAME_DATE)
    brooks_games_for_date_patched = scraped_data.get_bbref_games_for_date(PATCH_BB_DAILY_GAME_DATE)
    assert bbref_games_for_date_patched.game_count == brooks_games_for_date_patched.game_count
    assert (
        brooks_games_for_date_patched.all_bbref_game_ids
        == bbref_games_for_date_patched.all_bbref_game_ids
    )

    plogs_for_game_1_patched = update_scraped_pitch_logs(
        db_session=db_session,
        scraped_data=scraped_data,
        game_date=PATCH_BB_DAILY_GAME_DATE,
        bbref_game_id=PATCHED_BR_GAME_ID1,
        apply_patch_list=True,
    )
    assert plogs_for_game_1_patched.bbref_game_id == PATCHED_BR_GAME_ID1
    assert plogs_for_game_1_patched.pitch_log_count == 10
    assert all(plog.parsed_all_info for plog in plogs_for_game_1_patched.pitch_logs)

    pitch_apps_for_game_1_patched = (
        db_session.query(PitchAppScrapeStatus).filter_by(bbref_game_id=PATCHED_BR_GAME_ID1).all()
    )
    pitch_apps_for_game_1_invalid_id = (
        db_session.query(PitchAppScrapeStatus).filter_by(bbref_game_id=INVALID_BR_GAME_ID1).all()
    )
    pitch_apps_for_game_2_invalid_id = (
        db_session.query(PitchAppScrapeStatus).filter_by(bbref_game_id=INVALID_BR_GAME_ID2).all()
    )
    assert len(pitch_apps_for_game_1_patched) == 10
    assert not pitch_apps_for_game_1_invalid_id
    assert not pitch_apps_for_game_2_invalid_id

    game_status_patched_game_id = GameScrapeStatus.find_by_bbref_game_id(
        db_session, PATCHED_BR_GAME_ID1
    )
    game_status_invalid_game_id = GameScrapeStatus.find_by_bbref_game_id(
        db_session, INVALID_BR_GAME_ID1
    )
    game_status_remove_game_id = GameScrapeStatus.find_by_bbref_game_id(
        db_session, INVALID_BR_GAME_ID2
    )
    assert game_status_patched_game_id
    assert not game_status_invalid_game_id
    assert not game_status_remove_game_id
