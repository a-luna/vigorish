from pathlib import PosixPath

from vigorish.config.database import PitchAppScrapeStatus
from vigorish.data.process.combine_boxscore_and_pitchfx_for_game import combine_data
from vigorish.status.update_status_combined_data import update_pitch_app_audit_successful

BBREF_GAME_ID_NO_ERRORS = "TOR201906170"
BBREF_GAME_ID_WITH_ERRORS = "NYA201906112"
BBREF_GAME_ID_NO_PFX_FOR_PITCH_APP = "PIT201909070"


def test_combine_data_no_errors(db_session, scraped_data):
    result = combine_data(db_session, scraped_data, BBREF_GAME_ID_NO_ERRORS)
    assert result.success
    saved_file_dict = result.value
    json_filepath = saved_file_dict["local_filepath"]
    assert json_filepath and isinstance(json_filepath, PosixPath)
    result = scraped_data.json_storage.decode_json_combined_data_local_file(
        BBREF_GAME_ID_NO_ERRORS
    )
    assert result.success
    combined_data = result.value
    assert "pitchfx_vs_bbref_audit" in combined_data
    data_audit = combined_data["pitchfx_vs_bbref_audit"]
    assert "pitchfx_data_complete" in data_audit and data_audit["pitchfx_data_complete"]
    assert "total_batters_faced_bbref" in data_audit
    assert data_audit["total_batters_faced_bbref"] == 82
    assert "total_at_bats_pitchfx_complete" in data_audit
    assert data_audit["total_at_bats_pitchfx_complete"] == 82
    assert "total_at_bats_missing_pitchfx" in data_audit
    assert data_audit["total_at_bats_missing_pitchfx"] == 0
    assert "total_at_bats_extra_pitchfx" in data_audit
    assert data_audit["total_at_bats_extra_pitchfx"] == 0
    assert "pitch_count_bbref_stats_table" in data_audit
    assert data_audit["pitch_count_bbref_stats_table"] == 298
    assert "pitch_count_bbref_pitch_seq" in data_audit
    assert data_audit["pitch_count_bbref_pitch_seq"] == 298
    assert "pitch_count_pitchfx_audited" in data_audit
    assert data_audit["pitch_count_pitchfx_audited"] == 298
    assert "pitch_count_missing_pitchfx" in data_audit
    assert data_audit["pitch_count_missing_pitchfx"] == 0
    assert "missing_pitchfx_is_valid" in data_audit
    assert data_audit["missing_pitchfx_is_valid"]
    assert "at_bat_ids_missing_pitchfx" in data_audit
    assert data_audit["at_bat_ids_missing_pitchfx"] == []
    assert "at_bat_ids_pitchfx_data_error" in data_audit
    assert data_audit["at_bat_ids_pitchfx_data_error"] == []
    assert "duplicate_pitchfx_removed_count" in data_audit
    assert data_audit["duplicate_pitchfx_removed_count"] == 1

    result = update_pitch_app_audit_successful(db_session, scraped_data, BBREF_GAME_ID_NO_ERRORS)
    assert result.success
    pitch_app_status = PitchAppScrapeStatus.find_by_pitch_app_id(db_session, "TOR201906170_429719")
    assert pitch_app_status


def test_combine_data_with_errors(db_session, scraped_data):
    result = combine_data(db_session, scraped_data, BBREF_GAME_ID_WITH_ERRORS)
    assert result.success
    saved_file_dict = result.value
    json_filepath = saved_file_dict["local_filepath"]
    assert json_filepath and isinstance(json_filepath, PosixPath)
    result = scraped_data.json_storage.decode_json_combined_data_local_file(
        BBREF_GAME_ID_WITH_ERRORS
    )
    assert result.success
    combined_data = result.value
    assert "pitchfx_vs_bbref_audit" in combined_data
    data_audit = combined_data["pitchfx_vs_bbref_audit"]
    assert "pitchfx_data_complete" in data_audit and not data_audit["pitchfx_data_complete"]
    assert "total_batters_faced_bbref" in data_audit
    assert data_audit["total_batters_faced_bbref"] == 83
    assert "total_at_bats_pitchfx_complete" in data_audit
    assert data_audit["total_at_bats_pitchfx_complete"] == 78
    assert "total_at_bats_missing_pitchfx" in data_audit
    assert data_audit["total_at_bats_missing_pitchfx"] == 5
    assert "total_at_bats_extra_pitchfx" in data_audit
    assert data_audit["total_at_bats_extra_pitchfx"] == 0
    assert "pitch_count_bbref_stats_table" in data_audit
    assert data_audit["pitch_count_bbref_stats_table"] == 334
    assert "pitch_count_bbref_pitch_seq" in data_audit
    assert data_audit["pitch_count_bbref_pitch_seq"] == 334
    assert "pitch_count_pitchfx_audited" in data_audit
    assert data_audit["pitch_count_pitchfx_audited"] == 320
    assert "pitch_count_missing_pitchfx" in data_audit
    assert data_audit["pitch_count_missing_pitchfx"] == 14
    assert "missing_pitchfx_is_valid" in data_audit
    assert data_audit["missing_pitchfx_is_valid"]
    assert "at_bat_ids_missing_pitchfx" in data_audit
    assert len(data_audit["at_bat_ids_missing_pitchfx"]) == 5
    assert data_audit["at_bat_ids_missing_pitchfx"] == [
        "NYA201906112_3_NYN_450306_NYA_457727_0",
        "NYA201906112_3_NYN_450306_NYA_570482_0",
        "NYA201906112_3_NYN_450306_NYA_572228_0",
        "NYA201906112_4_NYA_664856_NYN_605204_0",
        "NYA201906112_4_NYA_664856_NYN_624424_0",
    ]
    assert "at_bat_ids_pitchfx_data_error" in data_audit
    assert data_audit["at_bat_ids_pitchfx_data_error"] == []
    assert "duplicate_pitchfx_removed_count" in data_audit
    assert data_audit["duplicate_pitchfx_removed_count"] == 1


def test_combine_data_no_pfx_for_pitch_app(db_session, scraped_data):
    result = combine_data(db_session, scraped_data, BBREF_GAME_ID_NO_PFX_FOR_PITCH_APP)
    assert result.success
    saved_file_dict = result.value
    json_filepath = saved_file_dict["local_filepath"]
    assert json_filepath and isinstance(json_filepath, PosixPath)
    result = scraped_data.json_storage.decode_json_combined_data_local_file(
        BBREF_GAME_ID_NO_PFX_FOR_PITCH_APP
    )
    assert result.success
    combined_data = result.value
    assert "pitchfx_vs_bbref_audit" in combined_data
    data_audit = combined_data["pitchfx_vs_bbref_audit"]
    assert "pitchfx_data_complete" in data_audit and not data_audit["pitchfx_data_complete"]
    assert "total_batters_faced_bbref" in data_audit
    assert data_audit["total_batters_faced_bbref"] == 79
    assert "total_at_bats_pitchfx_complete" in data_audit
    assert data_audit["total_at_bats_pitchfx_complete"] == 72
    assert "total_at_bats_missing_pitchfx" in data_audit
    assert data_audit["total_at_bats_missing_pitchfx"] == 7
    assert "total_at_bats_extra_pitchfx" in data_audit
    assert data_audit["total_at_bats_extra_pitchfx"] == 0
    assert "pitch_count_bbref_stats_table" in data_audit
    assert data_audit["pitch_count_bbref_stats_table"] == 310
    assert "pitch_count_bbref_pitch_seq" in data_audit
    assert data_audit["pitch_count_bbref_pitch_seq"] == 310
    assert "pitch_count_pitchfx_audited" in data_audit
    assert data_audit["pitch_count_pitchfx_audited"] == 276
    assert "pitch_count_missing_pitchfx" in data_audit
    assert data_audit["pitch_count_missing_pitchfx"] == 34
    assert "missing_pitchfx_is_valid" in data_audit
    assert data_audit["missing_pitchfx_is_valid"]
    assert "at_bat_ids_missing_pitchfx" in data_audit
    assert len(data_audit["at_bat_ids_missing_pitchfx"]) == 7
    assert data_audit["at_bat_ids_missing_pitchfx"] == [
        "PIT201909070_9_PIT_571917_SLN_592660_0",
        "PIT201909070_9_PIT_571917_SLN_664056_0",
        "PIT201909070_9_PIT_571917_SLN_668227_0",
        "PIT201909070_9_SLN_594965_PIT_571467_0",
        "PIT201909070_9_SLN_594965_PIT_591741_0",
        "PIT201909070_9_SLN_594965_PIT_592567_0",
        "PIT201909070_9_SLN_594965_PIT_622569_0",
    ]
    assert "at_bat_ids_pitchfx_data_error" in data_audit
    assert data_audit["at_bat_ids_pitchfx_data_error"] == []
    assert "duplicate_pitchfx_removed_count" in data_audit
    assert data_audit["duplicate_pitchfx_removed_count"] == 1
