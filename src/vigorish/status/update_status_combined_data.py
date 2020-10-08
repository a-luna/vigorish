from collections import defaultdict

from vigorish.config.database import PitchAppScrapeStatus
from vigorish.util.result import Result


def update_pitch_apps_for_game_combined_data(db_session, combined_data):
    pfx_errors = defaultdict(dict)
    combined_success = []
    away_team_pitch_stats = combined_data["away_team_data"]["pitching_stats"]
    home_team_pitch_stats = combined_data["home_team_data"]["pitching_stats"]
    all_pitch_stats = away_team_pitch_stats + home_team_pitch_stats
    for pitch_stats in all_pitch_stats:
        pitch_app_id = pitch_stats["pitch_app_id"]
        result = get_pitch_app_status(db_session, pitch_app_id)
        if result.failure:
            return result
        pitch_app_status = result.value
        pitch_app_status = update_pitch_app_combined_data(db_session, pitch_app_status, pitch_stats)
        pitchfx_audit = pitch_stats["pitch_app_pitchfx_audit"]
        if pitch_app_status.pitchfx_error:
            at_bat_ids = pitchfx_audit["at_bat_ids_pitchfx_error"]
            pfx_errors["pitchfx_error"][pitch_app_id] = at_bat_ids
        if pitch_app_status.invalid_pitchfx:
            at_bat_ids = pitchfx_audit["at_bat_ids_invalid_pitchfx"]
            pfx_errors["invalid_pitchfx"][pitch_app_id] = at_bat_ids
        if not (pitch_app_status.pitchfx_error or pitch_app_status.invalid_pitchfx):
            combined_success.append(pitch_app_id)
    db_session.commit()
    return Result.Ok({"combined_success": combined_success, "pfx_errors": pfx_errors})


def update_pitch_app_combined_data(db_session, pitch_app_status, pitch_stats):
    pitchfx_audit = pitch_stats["pitch_app_pitchfx_audit"]
    no_pitchfx_data = 1 if pitchfx_audit["pitch_count_pitchfx"] == 0 else 0
    pitchfx_error = 1 if pitchfx_audit["pitchfx_error"] else 0
    invalid_pitchfx = 1 if pitchfx_audit["invalid_pitchfx"] else 0
    pitch_app_status.combined_pitchfx_bbref_data = 1
    pitch_app_status.no_pitchfx_data = no_pitchfx_data
    pitch_app_status.batters_faced_bbref = pitchfx_audit["batters_faced_bbref"]
    pitch_app_status.batters_faced_pitchfx = pitchfx_audit["batters_faced_pitchfx"]
    pitch_app_status.duplicate_guid_removed_count = pitchfx_audit["duplicate_guid_removed_count"]
    pitch_app_status.pitch_count_bbref = pitchfx_audit["pitch_count_bbref"]
    pitch_app_status.pitch_count_pitchfx_audited = pitchfx_audit["pitch_count_pitchfx"]
    pitch_app_status.total_at_bats_pitchfx_complete = pitchfx_audit[
        "total_at_bats_pitchfx_complete"
    ]
    pitch_app_status.patched_pitchfx_count = pitchfx_audit["patched_pitchfx_count"]
    pitch_app_status.total_at_bats_patched_pitchfx = pitchfx_audit["total_at_bats_patched_pitchfx"]
    pitch_app_status.missing_pitchfx_count = pitchfx_audit["missing_pitchfx_count"]
    pitch_app_status.total_at_bats_missing_pitchfx = pitchfx_audit["total_at_bats_missing_pitchfx"]
    pitch_app_status.extra_pitchfx_count = pitchfx_audit["extra_pitchfx_count"]
    pitch_app_status.total_at_bats_extra_pitchfx = pitchfx_audit["total_at_bats_extra_pitchfx"]
    pitch_app_status.extra_pitchfx_removed_count = pitchfx_audit["extra_pitchfx_removed_count"]
    pitch_app_status.total_at_bats_extra_pitchfx_removed = pitchfx_audit[
        "total_at_bats_extra_pitchfx_removed"
    ]
    pitch_app_status.invalid_pitchfx = invalid_pitchfx
    pitch_app_status.invalid_pitchfx_count = pitchfx_audit["invalid_pitchfx_count"]
    pitch_app_status.total_at_bats_invalid_pitchfx = pitchfx_audit["total_at_bats_invalid_pitchfx"]
    pitch_app_status.pitchfx_error = pitchfx_error
    pitch_app_status.total_at_bats_pitchfx_error = pitchfx_audit["total_at_bats_pitchfx_error"]
    return pitch_app_status


def get_pitch_app_status(db_session, pitch_app_id):
    pitch_app_status = PitchAppScrapeStatus.find_by_pitch_app_id(db_session, pitch_app_id)
    if not pitch_app_status:
        error = (
            f"scrape_status_pitch_app does not contain an "
            f"entry for pitch_app_id: {pitch_app_id}"
        )
        return Result.Fail(error)
    return Result.Ok(pitch_app_status)
