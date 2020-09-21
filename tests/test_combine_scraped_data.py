from datetime import datetime

from vigorish.config.database import PitchAppScrapeStatus, GameScrapeStatus
from vigorish.status.update_status_combined_data import update_pitch_apps_for_game_combined_data

GAME_ID_NO_ERRORS = "TOR201906170"
NO_ERRORS_PITCH_APP = "TOR201906170_429719"
GAME_ID_WITH_ERRORS = "NYA201906112"
GAME_ID_NO_PFX_FOR_PITCH_APP = "PIT201909070"
GAME_ID_EXTRA_PFX_REMOVED = "TEX201904150"
COMBINED_DATA_GAME_DICT = {
    datetime(2019, 6, 17): {
        "bbref_game_id": GAME_ID_NO_ERRORS,
        "bb_game_id": "gid_2019_06_17_anamlb_tormlb_1",
    },
    datetime(2019, 6, 11): {
        "bbref_game_id": GAME_ID_WITH_ERRORS,
        "bb_game_id": "gid_2019_06_11_nynmlb_nyamlb_2",
    },
    datetime(2019, 9, 7): {
        "bbref_game_id": GAME_ID_NO_PFX_FOR_PITCH_APP,
        "bb_game_id": "gid_2019_09_07_slnmlb_pitmlb_1",
    },
    datetime(2019, 4, 15): {
        "bbref_game_id": GAME_ID_EXTRA_PFX_REMOVED,
        "bb_game_id": "gid_2019_04_15_anamlb_texmlb_1",
    },
}


def combine_scraped_data_for_game(db_session, scraped_data, bbref_game_id):
    game_status = GameScrapeStatus.find_by_bbref_game_id(db_session, bbref_game_id)
    boxscore = scraped_data.get_bbref_boxscore(bbref_game_id)
    pfx_logs = scraped_data.get_all_pitchfx_logs_for_game(bbref_game_id).value
    avg_pitch_times = scraped_data.get_avg_pitch_times()
    result = scraped_data.combine_data.execute(game_status, boxscore, pfx_logs, avg_pitch_times)
    assert result.success
    return result.value


def test_combine_data_no_errors(db_session, scraped_data):
    combined_data = combine_scraped_data_for_game(db_session, scraped_data, GAME_ID_NO_ERRORS)
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


def test_combine_data_with_errors(db_session, scraped_data):
    combined_data = combine_scraped_data_for_game(db_session, scraped_data, GAME_ID_WITH_ERRORS)
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


def test_combine_data_no_pfx_for_pitch_app(db_session, scraped_data):
    combined_data = combine_scraped_data_for_game(
        db_session, scraped_data, GAME_ID_NO_PFX_FOR_PITCH_APP
    )
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


def test_combine_data_extra_pitchfx_removed(db_session, scraped_data):
    combined_data = combine_scraped_data_for_game(
        db_session, scraped_data, GAME_ID_EXTRA_PFX_REMOVED
    )
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
