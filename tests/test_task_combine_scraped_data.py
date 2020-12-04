from tests.util import (
    combine_scraped_data_for_game,
    COMBINED_DATA_GAME_DICT,
    GAME_ID_EXTRA_PFX_REMOVED,
    GAME_ID_NO_ERRORS,
    GAME_ID_NO_PFX_FOR_PITCH_APP,
    GAME_ID_PATCH_BOXSCORE,
    GAME_ID_PATCH_PFX,
    GAME_ID_PFX_OUT_OF_SEQUENCE,
    GAME_ID_WITH_ERRORS,
    NO_ERRORS_PITCH_APP,
    update_scraped_bbref_games_for_date,
    update_scraped_boxscore,
    update_scraped_brooks_games_for_date,
    update_scraped_pitch_logs,
    update_scraped_pitchfx_logs,
)
from vigorish.database import GameScrapeStatus, PitchAppScrapeStatus
from vigorish.status.update_status_combined_data import update_pitch_apps_for_game_combined_data
from vigorish.tasks.combine_scraped_data import CombineScrapedDataTask
from vigorish.tasks.patch_all_invalid_pfx import PatchAllInvalidPitchFxTask
from vigorish.tasks.patch_invalid_pfx import PatchInvalidPitchFxTask
from vigorish.util.exceptions import ScrapedDataException


def create_test_data(db_session, scraped_data, test_id):
    game_date = COMBINED_DATA_GAME_DICT[test_id]["game_date"]
    bbref_game_id = COMBINED_DATA_GAME_DICT[test_id]["bbref_game_id"]
    bb_game_id = COMBINED_DATA_GAME_DICT[test_id]["bb_game_id"]
    update_scraped_bbref_games_for_date(db_session, scraped_data, game_date)
    update_scraped_brooks_games_for_date(db_session, scraped_data, game_date)
    update_scraped_boxscore(db_session, scraped_data, bbref_game_id)
    update_scraped_pitch_logs(db_session, scraped_data, game_date, bbref_game_id)
    update_scraped_pitchfx_logs(db_session, scraped_data, bb_game_id)
    db_session.commit()


def test_combine_data_no_errors(vig_app):
    db_session = vig_app.db_session
    scraped_data = vig_app.scraped_data
    test_id = "NO_ERRORS"
    create_test_data(db_session, scraped_data, test_id)
    combined_data = combine_scraped_data_for_game(vig_app, GAME_ID_NO_ERRORS)
    assert "pitchfx_vs_bbref_audit" in combined_data
    data_audit = combined_data["pitchfx_vs_bbref_audit"]
    assert "batters_faced_bbref" in data_audit
    assert data_audit["batters_faced_bbref"] == 82
    assert "total_at_bats_pitchfx_complete" in data_audit
    assert data_audit["total_at_bats_pitchfx_complete"] == 82
    assert "total_at_bats_missing_pitchfx" in data_audit
    assert data_audit["total_at_bats_missing_pitchfx"] == 0
    assert "total_at_bats_extra_pitchfx" in data_audit
    assert data_audit["total_at_bats_extra_pitchfx"] == 0
    assert "pitch_count_bbref_stats_table" in data_audit
    assert data_audit["pitch_count_bbref_stats_table"] == 298
    assert "pitch_count_bbref" in data_audit
    assert data_audit["pitch_count_bbref"] == 298
    assert "pitch_count_pitchfx" in data_audit
    assert data_audit["pitch_count_pitchfx"] == 298
    assert "missing_pitchfx_count" in data_audit
    assert data_audit["missing_pitchfx_count"] == 0
    assert "at_bat_ids_missing_pitchfx" in data_audit
    assert data_audit["at_bat_ids_missing_pitchfx"] == []
    assert "pitchfx_error" in data_audit
    assert not data_audit["pitchfx_error"]
    assert "at_bat_ids_pitchfx_error" in data_audit
    assert data_audit["at_bat_ids_pitchfx_error"] == []
    assert "duplicate_guid_removed_count" in data_audit
    assert data_audit["duplicate_guid_removed_count"] == 1

    pitch_app_status = PitchAppScrapeStatus.find_by_pitch_app_id(db_session, NO_ERRORS_PITCH_APP)
    assert pitch_app_status
    assert pitch_app_status.combined_pitchfx_bbref_data == 0
    assert pitch_app_status.pitch_count_pitch_log == 38
    assert pitch_app_status.pitch_count_bbref == 0
    assert pitch_app_status.pitch_count_pitchfx == 38
    assert pitch_app_status.pitch_count_pitchfx_audited == 0
    assert pitch_app_status.duplicate_guid_removed_count == 0
    assert pitch_app_status.missing_pitchfx_count == 0
    assert pitch_app_status.batters_faced_bbref == 0
    assert pitch_app_status.total_at_bats_pitchfx_complete == 0
    assert pitch_app_status.total_at_bats_missing_pitchfx == 0

    result = update_pitch_apps_for_game_combined_data(db_session, combined_data)
    assert result.success

    assert pitch_app_status.combined_pitchfx_bbref_data == 1
    assert pitch_app_status.pitch_count_pitch_log == 38
    assert pitch_app_status.pitch_count_bbref == 38
    assert pitch_app_status.pitch_count_pitchfx == 38
    assert pitch_app_status.pitch_count_pitchfx_audited == 38
    assert pitch_app_status.duplicate_guid_removed_count == 0
    assert pitch_app_status.missing_pitchfx_count == 0
    assert pitch_app_status.batters_faced_bbref == 10
    assert pitch_app_status.total_at_bats_pitchfx_complete == 10
    assert pitch_app_status.total_at_bats_missing_pitchfx == 0
    db_session.commit()


def test_combine_data_with_errors(vig_app):
    db_session = vig_app.db_session
    scraped_data = vig_app.scraped_data
    test_id = "WITH_ERRORS"
    create_test_data(db_session, scraped_data, test_id)
    combined_data = combine_scraped_data_for_game(vig_app, GAME_ID_WITH_ERRORS)
    assert "pitchfx_vs_bbref_audit" in combined_data
    data_audit = combined_data["pitchfx_vs_bbref_audit"]
    assert "batters_faced_bbref" in data_audit
    assert data_audit["batters_faced_bbref"] == 83
    assert "total_at_bats_pitchfx_complete" in data_audit
    assert data_audit["total_at_bats_pitchfx_complete"] == 78
    assert "total_at_bats_missing_pitchfx" in data_audit
    assert data_audit["total_at_bats_missing_pitchfx"] == 5
    assert "total_at_bats_extra_pitchfx" in data_audit
    assert data_audit["total_at_bats_extra_pitchfx"] == 0
    assert "pitch_count_bbref_stats_table" in data_audit
    assert data_audit["pitch_count_bbref_stats_table"] == 334
    assert "pitch_count_bbref" in data_audit
    assert data_audit["pitch_count_bbref"] == 334
    assert "pitch_count_pitchfx" in data_audit
    assert data_audit["pitch_count_pitchfx"] == 320
    assert "missing_pitchfx_count" in data_audit
    assert data_audit["missing_pitchfx_count"] == 14
    assert "at_bat_ids_missing_pitchfx" in data_audit
    assert len(data_audit["at_bat_ids_missing_pitchfx"]) == 5
    assert data_audit["at_bat_ids_missing_pitchfx"] == [
        "NYA201906112_03_NYN_450306_NYA_572228_0",
        "NYA201906112_03_NYN_450306_NYA_570482_0",
        "NYA201906112_03_NYN_450306_NYA_457727_0",
        "NYA201906112_04_NYA_664856_NYN_605204_0",
        "NYA201906112_04_NYA_664856_NYN_624424_0",
    ]
    assert "pitchfx_error" in data_audit
    assert not data_audit["pitchfx_error"]
    assert "at_bat_ids_pitchfx_error" in data_audit
    assert data_audit["at_bat_ids_pitchfx_error"] == []
    assert "duplicate_guid_removed_count" in data_audit
    assert data_audit["duplicate_guid_removed_count"] == 1
    db_session.commit()


def test_combine_data_no_pfx_for_pitch_app(vig_app):
    db_session = vig_app.db_session
    scraped_data = vig_app.scraped_data
    test_id = "NO_PFX_FOR_PITCH_APP"
    create_test_data(db_session, scraped_data, test_id)
    combined_data = combine_scraped_data_for_game(vig_app, GAME_ID_NO_PFX_FOR_PITCH_APP)
    assert "pitchfx_vs_bbref_audit" in combined_data
    data_audit = combined_data["pitchfx_vs_bbref_audit"]
    assert "batters_faced_bbref" in data_audit
    assert data_audit["batters_faced_bbref"] == 79
    assert "total_at_bats_pitchfx_complete" in data_audit
    assert data_audit["total_at_bats_pitchfx_complete"] == 72
    assert "total_at_bats_missing_pitchfx" in data_audit
    assert data_audit["total_at_bats_missing_pitchfx"] == 7
    assert "total_at_bats_extra_pitchfx" in data_audit
    assert data_audit["total_at_bats_extra_pitchfx"] == 0
    assert "pitch_count_bbref_stats_table" in data_audit
    assert data_audit["pitch_count_bbref_stats_table"] == 310
    assert "pitch_count_bbref" in data_audit
    assert data_audit["pitch_count_bbref"] == 310
    assert "pitch_count_pitchfx" in data_audit
    assert data_audit["pitch_count_pitchfx"] == 276
    assert "missing_pitchfx_count" in data_audit
    assert data_audit["missing_pitchfx_count"] == 34
    assert "at_bat_ids_missing_pitchfx" in data_audit
    assert len(data_audit["at_bat_ids_missing_pitchfx"]) == 7
    assert data_audit["at_bat_ids_missing_pitchfx"] == [
        "PIT201909070_09_PIT_571917_SLN_664056_0",
        "PIT201909070_09_PIT_571917_SLN_592660_0",
        "PIT201909070_09_PIT_571917_SLN_668227_0",
        "PIT201909070_09_SLN_594965_PIT_622569_0",
        "PIT201909070_09_SLN_594965_PIT_571467_0",
        "PIT201909070_09_SLN_594965_PIT_592567_0",
        "PIT201909070_09_SLN_594965_PIT_591741_0",
    ]
    assert "pitchfx_error" in data_audit
    assert not data_audit["pitchfx_error"]
    assert "at_bat_ids_pitchfx_error" in data_audit
    assert data_audit["at_bat_ids_pitchfx_error"] == []
    assert "duplicate_guid_removed_count" in data_audit
    assert data_audit["duplicate_guid_removed_count"] == 1
    db_session.commit()


def test_combine_data_extra_pitchfx_removed(vig_app):
    db_session = vig_app.db_session
    scraped_data = vig_app.scraped_data
    test_id = "EXTRA_PFX_REMOVED"
    create_test_data(db_session, scraped_data, test_id)
    combined_data = combine_scraped_data_for_game(vig_app, GAME_ID_EXTRA_PFX_REMOVED)
    assert "pitchfx_vs_bbref_audit" in combined_data
    data_audit = combined_data["pitchfx_vs_bbref_audit"]
    assert "batters_faced_bbref" in data_audit
    assert data_audit["batters_faced_bbref"] == 90
    assert "total_at_bats_pitchfx_complete" in data_audit
    assert data_audit["total_at_bats_pitchfx_complete"] == 90
    assert "total_at_bats_missing_pitchfx" in data_audit
    assert data_audit["total_at_bats_missing_pitchfx"] == 0
    assert "total_at_bats_extra_pitchfx" in data_audit
    assert data_audit["total_at_bats_extra_pitchfx"] == 0
    assert "total_at_bats_extra_pitchfx_removed" in data_audit
    assert data_audit["total_at_bats_extra_pitchfx_removed"] == 2
    assert "pitch_count_bbref_stats_table" in data_audit
    assert data_audit["pitch_count_bbref_stats_table"] == 358
    assert "pitch_count_bbref" in data_audit
    assert data_audit["pitch_count_bbref"] == 358
    assert "pitch_count_pitchfx" in data_audit
    assert data_audit["pitch_count_pitchfx"] == 358
    assert "missing_pitchfx_count" in data_audit
    assert data_audit["missing_pitchfx_count"] == 0
    assert "at_bat_ids_missing_pitchfx" in data_audit
    assert len(data_audit["at_bat_ids_missing_pitchfx"]) == 0
    assert data_audit["at_bat_ids_missing_pitchfx"] == []
    assert "pitchfx_error" in data_audit
    assert not data_audit["pitchfx_error"]
    assert "at_bat_ids_pitchfx_error" in data_audit
    assert data_audit["at_bat_ids_pitchfx_error"] == []
    assert "at_bat_ids_extra_pitchfx_removed" in data_audit
    assert data_audit["at_bat_ids_extra_pitchfx_removed"] == [
        "TEX201904150_05_TEX_571946_ANA_592743_0",
        "TEX201904150_05_TEX_571946_ANA_405395_0",
    ]
    assert "duplicate_guid_removed_count" in data_audit
    assert data_audit["duplicate_guid_removed_count"] == 1
    db_session.commit()


def test_combine_patched_pitchfx_data(vig_app):
    try:
        result = vig_app.scraped_data.json_storage.get_brooks_pitchfx_patch_list_local_file(
            GAME_ID_PATCH_PFX
        )
        if result.success and result.value.exists():
            result.value.unlink()
    except ScrapedDataException:
        pass

    test_id = "PATCH_PFX"
    create_test_data(vig_app.db_session, vig_app.scraped_data, test_id)
    combined_data = combine_scraped_data_for_game(vig_app, GAME_ID_PATCH_PFX)
    assert "pitchfx_vs_bbref_audit" in combined_data
    data_audit = combined_data["pitchfx_vs_bbref_audit"]
    assert "batters_faced_bbref" in data_audit
    assert data_audit["batters_faced_bbref"] == 78
    assert "batters_faced_pitchfx" in data_audit
    assert data_audit["batters_faced_pitchfx"] == 75
    assert "total_at_bats_pitchfx_complete" in data_audit
    assert data_audit["total_at_bats_pitchfx_complete"] == 75
    assert "total_at_bats_patched_pitchfx" in data_audit
    assert data_audit["total_at_bats_patched_pitchfx"] == 0
    assert "total_at_bats_missing_pitchfx" in data_audit
    assert data_audit["total_at_bats_missing_pitchfx"] == 3
    assert "total_at_bats_extra_pitchfx" in data_audit
    assert data_audit["total_at_bats_extra_pitchfx"] == 0
    assert "total_at_bats_extra_pitchfx_removed" in data_audit
    assert data_audit["total_at_bats_extra_pitchfx_removed"] == 0
    assert "total_at_bats_duplicate_guid_removed" in data_audit
    assert data_audit["total_at_bats_duplicate_guid_removed"] == 11
    assert "pitch_count_bbref_stats_table" in data_audit
    assert data_audit["pitch_count_bbref_stats_table"] == 329
    assert "pitch_count_bbref" in data_audit
    assert data_audit["pitch_count_bbref"] == 329
    assert "pitch_count_pitchfx" in data_audit
    assert data_audit["pitch_count_pitchfx"] == 313
    assert "patched_pitchfx_count" in data_audit
    assert data_audit["patched_pitchfx_count"] == 0
    assert "missing_pitchfx_count" in data_audit
    assert data_audit["missing_pitchfx_count"] == 16
    assert "duplicate_guid_removed_count" in data_audit
    assert data_audit["duplicate_guid_removed_count"] == 18
    assert "at_bat_ids_patched_pitchfx" in data_audit
    assert data_audit["at_bat_ids_patched_pitchfx"] == []
    assert "at_bat_ids_missing_pitchfx" in data_audit
    assert data_audit["at_bat_ids_missing_pitchfx"] == [
        "OAK201904030_08_OAK_465657_BOS_646240_0",
        "OAK201904030_08_OAK_465657_BOS_502110_0",
        "OAK201904030_08_OAK_465657_BOS_519048_0",
    ]
    assert "pitchfx_error" in data_audit
    assert not data_audit["pitchfx_error"]
    assert "at_bat_ids_pitchfx_error" in data_audit
    assert data_audit["at_bat_ids_pitchfx_error"] == []
    assert "at_bat_ids_extra_pitchfx_removed" in data_audit
    assert data_audit["at_bat_ids_extra_pitchfx_removed"] == []
    assert "at_bat_ids_duplicate_guid_removed" in data_audit
    assert data_audit["at_bat_ids_duplicate_guid_removed"] == [
        "OAK201904030_01_OAK_462136_BOS_605141_0",
        "OAK201904030_01_OAK_462136_BOS_643217_0",
        "OAK201904030_01_OAK_462136_BOS_646240_0",
        "OAK201904030_01_BOS_543135_OAK_543257_0",
        "OAK201904030_01_BOS_543135_OAK_656305_0",
        "OAK201904030_01_BOS_543135_OAK_572039_0",
        "OAK201904030_01_BOS_543135_OAK_501981_0",
        "OAK201904030_04_OAK_462136_BOS_646240_0",
        "OAK201904030_06_BOS_605155_OAK_657656_0",
        "OAK201904030_08_BOS_598264_OAK_434778_0",
        "OAK201904030_08_BOS_598264_OAK_595777_0",
    ]
    assert "invalid_pitchfx" in data_audit
    assert data_audit["invalid_pitchfx"]
    assert "at_bat_ids_invalid_pitchfx" in data_audit
    assert "OAK201904030_08_OAK_605525_BOS_519048_0" in data_audit["at_bat_ids_invalid_pitchfx"]
    assert "OAK201904030_08_OAK_605525_BOS_646240_0" in data_audit["at_bat_ids_invalid_pitchfx"]
    assert "OAK201904030_08_OAK_605525_BOS_502110_0" in data_audit["at_bat_ids_invalid_pitchfx"]

    patch_invalid_pfx = PatchInvalidPitchFxTask(vig_app)
    result = patch_invalid_pfx.execute(GAME_ID_PATCH_PFX, no_prompts=True)
    assert result.success
    patch_results = result.value
    assert patch_results["created_patch_list"]
    assert patch_results["fixed_all_errors"]
    combined_data = vig_app.scraped_data.get_combined_game_data(GAME_ID_PATCH_PFX)
    assert "pitchfx_vs_bbref_audit" in combined_data
    data_audit = combined_data["pitchfx_vs_bbref_audit"]
    assert "batters_faced_bbref" in data_audit
    assert data_audit["batters_faced_bbref"] == 78
    assert "batters_faced_pitchfx" in data_audit
    assert data_audit["batters_faced_pitchfx"] == 78
    assert "total_at_bats_pitchfx_complete" in data_audit
    assert data_audit["total_at_bats_pitchfx_complete"] == 78
    assert "total_at_bats_patched_pitchfx" in data_audit
    assert data_audit["total_at_bats_patched_pitchfx"] == 3
    assert "total_at_bats_missing_pitchfx" in data_audit
    assert data_audit["total_at_bats_missing_pitchfx"] == 0
    assert "total_at_bats_extra_pitchfx" in data_audit
    assert data_audit["total_at_bats_extra_pitchfx"] == 0
    assert "total_at_bats_extra_pitchfx_removed" in data_audit
    assert data_audit["total_at_bats_extra_pitchfx_removed"] == 0
    assert "total_at_bats_duplicate_guid_removed" in data_audit
    assert data_audit["total_at_bats_duplicate_guid_removed"] == 12
    assert "pitch_count_bbref_stats_table" in data_audit
    assert data_audit["pitch_count_bbref_stats_table"] == 329
    assert "pitch_count_bbref" in data_audit
    assert data_audit["pitch_count_bbref"] == 329
    assert "pitch_count_pitchfx" in data_audit
    assert data_audit["pitch_count_pitchfx"] == 329
    assert "patched_pitchfx_count" in data_audit
    assert data_audit["patched_pitchfx_count"] == 16
    assert "missing_pitchfx_count" in data_audit
    assert data_audit["missing_pitchfx_count"] == 0
    assert "duplicate_guid_removed_count" in data_audit
    assert data_audit["duplicate_guid_removed_count"] == 18
    assert "at_bat_ids_patched_pitchfx" in data_audit
    assert "OAK201904030_08_OAK_465657_BOS_519048_0" in data_audit["at_bat_ids_patched_pitchfx"]
    assert "OAK201904030_08_OAK_465657_BOS_646240_0" in data_audit["at_bat_ids_patched_pitchfx"]
    assert "OAK201904030_08_OAK_465657_BOS_502110_0" in data_audit["at_bat_ids_patched_pitchfx"]
    assert "at_bat_ids_missing_pitchfx" in data_audit
    assert data_audit["at_bat_ids_missing_pitchfx"] == []
    assert "pitchfx_error" in data_audit
    assert not data_audit["pitchfx_error"]
    assert "at_bat_ids_pitchfx_error" in data_audit
    assert data_audit["at_bat_ids_pitchfx_error"] == []
    assert "at_bat_ids_extra_pitchfx_removed" in data_audit
    assert data_audit["at_bat_ids_extra_pitchfx_removed"] == []
    assert "at_bat_ids_duplicate_guid_removed" in data_audit
    assert data_audit["at_bat_ids_duplicate_guid_removed"] == [
        "OAK201904030_01_OAK_462136_BOS_605141_0",
        "OAK201904030_01_OAK_462136_BOS_643217_0",
        "OAK201904030_01_OAK_462136_BOS_646240_0",
        "OAK201904030_01_BOS_543135_OAK_543257_0",
        "OAK201904030_01_BOS_543135_OAK_656305_0",
        "OAK201904030_01_BOS_543135_OAK_572039_0",
        "OAK201904030_01_BOS_543135_OAK_501981_0",
        "OAK201904030_04_OAK_462136_BOS_646240_0",
        "OAK201904030_06_BOS_605155_OAK_657656_0",
        "OAK201904030_08_OAK_465657_BOS_502110_0",
        "OAK201904030_08_BOS_598264_OAK_434778_0",
        "OAK201904030_08_BOS_598264_OAK_595777_0",
    ]
    assert "invalid_pitchfx" in data_audit
    assert not data_audit["invalid_pitchfx"]
    assert "at_bat_ids_invalid_pitchfx" in data_audit
    assert data_audit["at_bat_ids_invalid_pitchfx"] == []
    vig_app.db_session.commit()


def test_patch_all_invalid_pitchfx_data(vig_app):
    test_id = "PATCH_PFX"
    create_test_data(vig_app.db_session, vig_app.scraped_data, test_id)
    result = vig_app.scraped_data.json_storage.get_brooks_pitchfx_patch_list_local_file(
        GAME_ID_PATCH_PFX
    )
    if result.success and result.value.exists():
        result.value.unlink()
    combined_data = combine_scraped_data_for_game(vig_app, GAME_ID_PATCH_PFX)
    assert "pitchfx_vs_bbref_audit" in combined_data
    data_audit = combined_data["pitchfx_vs_bbref_audit"]
    assert "batters_faced_bbref" in data_audit
    assert data_audit["batters_faced_bbref"] == 78
    assert "batters_faced_pitchfx" in data_audit
    assert data_audit["batters_faced_pitchfx"] == 75
    assert "total_at_bats_pitchfx_complete" in data_audit
    assert data_audit["total_at_bats_pitchfx_complete"] == 75
    assert "total_at_bats_patched_pitchfx" in data_audit
    assert data_audit["total_at_bats_patched_pitchfx"] == 0
    assert "total_at_bats_missing_pitchfx" in data_audit
    assert data_audit["total_at_bats_missing_pitchfx"] == 3
    assert "total_at_bats_extra_pitchfx" in data_audit
    assert data_audit["total_at_bats_extra_pitchfx"] == 0
    assert "total_at_bats_extra_pitchfx_removed" in data_audit
    assert data_audit["total_at_bats_extra_pitchfx_removed"] == 0
    assert "total_at_bats_duplicate_guid_removed" in data_audit
    assert data_audit["total_at_bats_duplicate_guid_removed"] == 11
    assert "pitch_count_bbref_stats_table" in data_audit
    assert data_audit["pitch_count_bbref_stats_table"] == 329
    assert "pitch_count_bbref" in data_audit
    assert data_audit["pitch_count_bbref"] == 329
    assert "pitch_count_pitchfx" in data_audit
    assert data_audit["pitch_count_pitchfx"] == 313
    assert "patched_pitchfx_count" in data_audit
    assert data_audit["patched_pitchfx_count"] == 0
    assert "missing_pitchfx_count" in data_audit
    assert data_audit["missing_pitchfx_count"] == 16
    assert "duplicate_guid_removed_count" in data_audit
    assert data_audit["duplicate_guid_removed_count"] == 18
    assert "at_bat_ids_patched_pitchfx" in data_audit
    assert data_audit["at_bat_ids_patched_pitchfx"] == []
    assert "at_bat_ids_missing_pitchfx" in data_audit
    assert data_audit["at_bat_ids_missing_pitchfx"] == [
        "OAK201904030_08_OAK_465657_BOS_646240_0",
        "OAK201904030_08_OAK_465657_BOS_502110_0",
        "OAK201904030_08_OAK_465657_BOS_519048_0",
    ]
    assert "pitchfx_error" in data_audit
    assert not data_audit["pitchfx_error"]
    assert "at_bat_ids_pitchfx_error" in data_audit
    assert data_audit["at_bat_ids_pitchfx_error"] == []
    assert "at_bat_ids_extra_pitchfx_removed" in data_audit
    assert data_audit["at_bat_ids_extra_pitchfx_removed"] == []
    assert "at_bat_ids_duplicate_guid_removed" in data_audit
    assert data_audit["at_bat_ids_duplicate_guid_removed"] == [
        "OAK201904030_01_OAK_462136_BOS_605141_0",
        "OAK201904030_01_OAK_462136_BOS_643217_0",
        "OAK201904030_01_OAK_462136_BOS_646240_0",
        "OAK201904030_01_BOS_543135_OAK_543257_0",
        "OAK201904030_01_BOS_543135_OAK_656305_0",
        "OAK201904030_01_BOS_543135_OAK_572039_0",
        "OAK201904030_01_BOS_543135_OAK_501981_0",
        "OAK201904030_04_OAK_462136_BOS_646240_0",
        "OAK201904030_06_BOS_605155_OAK_657656_0",
        "OAK201904030_08_BOS_598264_OAK_434778_0",
        "OAK201904030_08_BOS_598264_OAK_595777_0",
    ]
    assert "invalid_pitchfx" in data_audit
    assert data_audit["invalid_pitchfx"]
    assert "at_bat_ids_invalid_pitchfx" in data_audit
    assert "OAK201904030_08_OAK_605525_BOS_519048_0" in data_audit["at_bat_ids_invalid_pitchfx"]
    assert "OAK201904030_08_OAK_605525_BOS_646240_0" in data_audit["at_bat_ids_invalid_pitchfx"]
    assert "OAK201904030_08_OAK_605525_BOS_502110_0" in data_audit["at_bat_ids_invalid_pitchfx"]

    game_status = GameScrapeStatus.find_by_bbref_game_id(vig_app.db_session, GAME_ID_PATCH_PFX)
    game_status.combined_data_success = 1
    game_status.combined_data_fail = 0
    vig_app.db_session.commit()
    result = update_pitch_apps_for_game_combined_data(vig_app.db_session, combined_data)
    assert result.success

    patch_all_invalid_pfx = PatchAllInvalidPitchFxTask(vig_app)
    result = patch_all_invalid_pfx.execute(2019)
    assert result.success
    patch_results_dict = result.value
    assert "all_patch_results" in patch_results_dict
    assert GAME_ID_PATCH_PFX in patch_results_dict["all_patch_results"]
    vig_app.db_session.commit()


def test_combine_patched_boxscore_data(vig_app):
    db_session = vig_app.db_session
    scraped_data = vig_app.scraped_data
    test_id = "PATCH_BOXSCORE"
    create_test_data(db_session, scraped_data, test_id)
    result = CombineScrapedDataTask(vig_app).execute(
        GAME_ID_PATCH_BOXSCORE, apply_patch_list=False, write_json=False, update_db=False
    )
    assert not result["combined_data_success"]
    assert "Invalid pitch abbreviation: a\nKeyError('a')" in result["error"]
    combined_data = combine_scraped_data_for_game(
        vig_app, GAME_ID_PATCH_BOXSCORE, apply_patch_list=True
    )
    assert "pitchfx_vs_bbref_audit" in combined_data


def test_find_pfx_out_of_sequence(vig_app):
    db_session = vig_app.db_session
    scraped_data = vig_app.scraped_data
    test_id = "PFX_OUT_OF_SEQUENCE"
    create_test_data(db_session, scraped_data, test_id)
    combined_data = combine_scraped_data_for_game(vig_app, GAME_ID_PFX_OUT_OF_SEQUENCE)
    assert "pitchfx_vs_bbref_audit" in combined_data
    data_audit = combined_data["pitchfx_vs_bbref_audit"]
    assert "batters_faced_bbref" in data_audit
    assert data_audit["batters_faced_bbref"] == 86
    assert "total_at_bats_pitchfx_complete" in data_audit
    assert data_audit["total_at_bats_pitchfx_complete"] == 80
    assert "total_at_bats_missing_pitchfx" in data_audit
    assert data_audit["total_at_bats_missing_pitchfx"] == 6
    assert "total_at_bats_extra_pitchfx" in data_audit
    assert data_audit["total_at_bats_extra_pitchfx"] == 0
    assert "total_at_bats_invalid_pitchfx" in data_audit
    assert data_audit["total_at_bats_invalid_pitchfx"] == 6
    assert "total_at_bats_extra_pitchfx_removed" in data_audit
    assert data_audit["total_at_bats_extra_pitchfx_removed"] == 2
    assert "total_at_bats_duplicate_guid_removed" in data_audit
    assert data_audit["total_at_bats_duplicate_guid_removed"] == 13
    assert "pitch_count_bbref_stats_table" in data_audit
    assert data_audit["pitch_count_bbref_stats_table"] == 344
    assert "pitch_count_bbref" in data_audit
    assert data_audit["pitch_count_bbref"] == 344
    assert "pitch_count_pitchfx" in data_audit
    assert data_audit["pitch_count_pitchfx"] == 322
    assert "missing_pitchfx_count" in data_audit
    assert data_audit["missing_pitchfx_count"] == 22
    assert "extra_pitchfx_count" in data_audit
    assert data_audit["extra_pitchfx_count"] == 0
    assert "invalid_pitchfx_count" in data_audit
    assert data_audit["invalid_pitchfx_count"] == 19
    assert "extra_pitchfx_removed_count" in data_audit
    assert data_audit["extra_pitchfx_removed_count"] == 3
    assert "duplicate_guid_removed_count" in data_audit
    assert data_audit["duplicate_guid_removed_count"] == 35
    assert "at_bat_ids_missing_pitchfx" in data_audit
    assert data_audit["at_bat_ids_missing_pitchfx"] == [
        "WAS201904030_06_WAS_502522_PHI_457705_0",
        "WAS201904030_06_PHI_504379_WAS_594694_0",
        "WAS201904030_06_PHI_504379_WAS_543228_0",
        "WAS201904030_06_PHI_504379_WAS_645302_0",
        "WAS201904030_06_PHI_504379_WAS_594809_0",
        "WAS201904030_06_PHI_504379_WAS_572821_0",
    ]
    assert "at_bat_ids_extra_pitchfx_removed" in data_audit
    assert data_audit["at_bat_ids_extra_pitchfx_removed"] == [
        "WAS201904030_06_WAS_502522_PHI_596748_0",
        "WAS201904030_06_WAS_502522_PHI_664068_0",
    ]
    assert "at_bat_ids_duplicate_guid_removed" in data_audit
    assert data_audit["at_bat_ids_duplicate_guid_removed"] == [
        "WAS201904030_01_WAS_434671_PHI_457705_0",
        "WAS201904030_01_WAS_434671_PHI_516416_0",
        "WAS201904030_01_WAS_434671_PHI_547180_0",
        "WAS201904030_01_WAS_434671_PHI_656555_0",
        "WAS201904030_01_WAS_434671_PHI_592663_0",
        "WAS201904030_01_WAS_434671_PHI_546318_0",
        "WAS201904030_01_PHI_605400_WAS_594809_0",
        "WAS201904030_01_PHI_605400_WAS_543685_0",
        "WAS201904030_01_PHI_605400_WAS_665742_0",
        "WAS201904030_01_PHI_605400_WAS_475582_0",
        "WAS201904030_01_PHI_605400_WAS_435559_0",
        "WAS201904030_03_PHI_605400_WAS_594694_0",
        "WAS201904030_08_PHI_622554_WAS_645302_0",
    ]
    assert "pitchfx_error" in data_audit
    assert not data_audit["pitchfx_error"]
    assert "at_bat_ids_pitchfx_error" in data_audit
    assert data_audit["at_bat_ids_pitchfx_error"] == []
    assert "at_bat_ids_invalid_pitchfx" in data_audit
    assert data_audit["at_bat_ids_invalid_pitchfx"] == [
        "WAS201904030_05_PHI_504379_WAS_594694_0",
        "WAS201904030_06_WAS_502522_PHI_516416_0",
        "WAS201904030_06_WAS_502522_PHI_547180_0",
        "WAS201904030_06_PHI_591693_WAS_645302_0",
        "WAS201904030_06_PHI_591693_WAS_594809_0",
        "WAS201904030_06_PHI_591693_WAS_572821_0",
    ]
