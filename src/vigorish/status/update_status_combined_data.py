from vigorish.config.database import PitchAppScrapeStatus
from vigorish.util.list_helpers import flatten_list2d
from vigorish.util.result import Result


def update_pitch_apps_for_game_combined_data(db_session, combined_data):
    audit_results = {}
    audit_results["success"] = []
    audit_results["failed"] = []
    away_team_pitch_stats = combined_data["away_team_data"]["pitching_stats"]
    home_team_pitch_stats = combined_data["home_team_data"]["pitching_stats"]
    all_pitch_stats = away_team_pitch_stats + home_team_pitch_stats
    for pitch_stats in all_pitch_stats:
        result = update_pitch_app_combined_data(db_session, pitch_stats)
        if result.failure:
            return result
        pitch_app_status = result.value
        if pitch_app_status.pitchfx_data_error:
            audit_results["failed"].append(pitch_stats["pitch_app_id"])
        else:
            audit_results["success"].append(pitch_stats["pitch_app_id"])
    return Result.Ok(audit_results)


def update_pitch_app_combined_data(db_session, pitch_stats):
    result = get_pitch_app_status(db_session, pitch_stats["pitch_app_id"])
    if result.failure:
        return result
    pitch_app_status = result.value
    pitchfx_data_error = 1 if pitch_stats["pitchfx_data_error"] else 0
    setattr(pitch_app_status, "combined_pitchfx_bbref_data", 1)
    setattr(pitch_app_status, "pitch_count_bbref", pitch_stats["pitch_count_bbref"])
    setattr(pitch_app_status, "pitch_count_pitchfx_audited", pitch_stats["pitch_count_pitchfx"])
    setattr(pitch_app_status, "missing_pitchfx_count", pitch_stats["missing_pitchfx_count"])
    setattr(pitch_app_status, "extra_pitchfx_count", pitch_stats["extra_pitchfx_count"])
    setattr(
        pitch_app_status,
        "duplicate_pitchfx_removed_count",
        pitch_stats["duplicate_pitchfx_removed_count"],
    )
    setattr(
        pitch_app_status, "extra_pitchfx_removed_count", pitch_stats["extra_pitchfx_removed_count"]
    )
    setattr(pitch_app_status, "pitchfx_data_error", pitchfx_data_error)
    setattr(pitch_app_status, "batters_faced_bbref", pitch_stats["batters_faced_bbref"])
    setattr(pitch_app_status, "batters_faced_pitchfx", pitch_stats["batters_faced_pitchfx"])
    setattr(
        pitch_app_status,
        "total_at_bats_pitchfx_complete",
        pitch_stats["total_at_bats_pitchfx_complete"],
    )
    setattr(
        pitch_app_status,
        "total_at_bats_missing_pitchfx",
        pitch_stats["total_at_bats_missing_pitchfx"],
    )
    setattr(
        pitch_app_status, "total_at_bats_extra_pitchfx", pitch_stats["total_at_bats_extra_pitchfx"]
    )
    setattr(
        pitch_app_status,
        "total_at_bats_pitchfx_data_error",
        pitch_stats["total_at_bats_pitchfx_data_error"],
    )
    db_session.commit()
    return Result.Ok(pitch_app_status)


def get_pitch_app_status(db_session, pitch_app_id):
    pitch_app_status = PitchAppScrapeStatus.find_by_pitch_app_id(db_session, pitch_app_id)
    if not pitch_app_status:
        error = (
            f"scrape_status_pitch_app does not contain an "
            f"entry for pitch_app_id: {pitch_app_id}"
        )
        return Result.Fail(error)
    return Result.Ok(pitch_app_status)
