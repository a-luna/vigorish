from vigorish.config.database import PitchAppScrapeStatus
from vigorish.util.list_helpers import flatten_list2d
from vigorish.util.result import Result


def update_pitch_apps_for_game_audit_successful(db_session, scraped_data, bbref_game_id):
    combined_data = scraped_data.get_json_combined_data(bbref_game_id)
    if not combined_data:
        return Result.Fail(f"Failed to retrieve combined data file for {bbref_game_id}")
    audit_results = {}
    audit_results["success"] = []
    audit_results["failed"] = []
    pbp_data = combined_data["play_by_play_data"]
    all_inning_events = flatten_list2d([pbp["inning_events"] for pbp in pbp_data])
    away_team_pitch_stats = combined_data["away_team_data"]["pitching_stats"]
    home_team_pitch_stats = combined_data["home_team_data"]["pitching_stats"]
    all_pitch_stats = away_team_pitch_stats + home_team_pitch_stats
    for pitch_stats in all_pitch_stats:
        pitch_app_id = pitch_stats["pitch_app_id"]
        at_bat_ids = pitch_stats["at_bat_ids"]
        at_bat_data = [event for event in all_inning_events if event["at_bat_id"] in at_bat_ids]
        result = update_pitch_app_combined_data(db_session, pitch_app_id, pitch_stats, at_bat_data)
        if result.failure:
            return result
        pitch_app_status = result.value
        if pitch_app_status.missing_pitchfx_is_valid:
            audit_results["success"].append(pitch_app_id)
        else:
            audit_results["failed"].append(pitch_app_id)
    return Result.Ok(audit_results)


def update_pitch_app_combined_data(db_session, pitch_app_id, pitch_stats, at_bat_data):
    result = get_pitch_app_status(db_session, pitch_app_id)
    if result.failure:
        return result
    pitch_app_status = result.value
    pitch_count_missing_pitchfx = sum(
        at_bat["missing_pitchfx_count"]
        for at_bat in at_bat_data
        if at_bat["missing_pitchfx_count"] > 0
    )
    pitch_count_extra_pitchfx = abs(
        sum(
            at_bat["missing_pitchfx_count"]
            for at_bat in at_bat_data
            if at_bat["missing_pitchfx_count"] < 0
        )
    )
    missing_pitchfx_is_valid = (
        1 if all(at_bat["missing_pitchfx_is_valid"] for at_bat in at_bat_data) else 0
    )
    incorrect_extra_pitchfx_removed_count = sum(
        at_bat["incorrect_extra_pitchfx_removed_count"] for at_bat in at_bat_data
    )
    total_at_bats_missing_pitchfx = len(
        [at_bat for at_bat in at_bat_data if at_bat["missing_pitchfx_count"] > 0]
    )
    total_at_bats_extra_pitchfx = abs(
        len([at_bat for at_bat in at_bat_data if at_bat["missing_pitchfx_count"] < 0])
    )
    total_at_bats_pitchfx_complete = (
        pitch_stats["batters_faced_pitchfx"]
        - total_at_bats_missing_pitchfx
        - total_at_bats_extra_pitchfx
    )
    setattr(pitch_app_status, "audit_successful", 1)
    setattr(pitch_app_status, "audit_failed", 0)
    setattr(pitch_app_status, "pitch_count_bbref", pitch_stats["total_pitch_count_bbref"])
    setattr(
        pitch_app_status, "pitch_count_pitchfx_audited", pitch_stats["total_pitch_count_pitchfx"]
    )
    setattr(
        pitch_app_status,
        "duplicate_pitchfx_removed_count",
        pitch_stats["pitchfx_data"]["duplicate_pitches_removed_count"],
    )
    setattr(
        pitch_app_status,
        "incorrect_extra_pitchfx_removed_count",
        incorrect_extra_pitchfx_removed_count,
    )
    setattr(pitch_app_status, "batters_faced_bbref", pitch_stats["batters_faced_bbref"])
    setattr(
        pitch_app_status, "pitch_count_missing_pitchfx", pitch_count_missing_pitchfx,
    )
    setattr(
        pitch_app_status, "pitch_count_extra_pitchfx", pitch_count_extra_pitchfx,
    )
    setattr(
        pitch_app_status, "missing_pitchfx_is_valid", missing_pitchfx_is_valid,
    )
    setattr(
        pitch_app_status, "total_at_bats_missing_pitchfx", total_at_bats_missing_pitchfx,
    )
    setattr(
        pitch_app_status, "total_at_bats_extra_pitchfx", total_at_bats_extra_pitchfx,
    )
    setattr(
        pitch_app_status, "total_at_bats_pitchfx_complete", total_at_bats_pitchfx_complete,
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


def update_pitch_appearances_audit_failed(db_session, pitch_app_ids):
    for pitch_app_id in pitch_app_ids:
        result = get_pitch_app_status(db_session, pitch_app_id)
        if result.failure:
            return result
        pitch_app_status = result.value
        setattr(pitch_app_status, "audit_successful", 0)
        setattr(pitch_app_status, "audit_failed", 1)
        setattr(pitch_app_status, "pitch_count_bbref", 0)
        setattr(pitch_app_status, "pitch_count_pitchfx_audited", 0)
        setattr(pitch_app_status, "duplicate_pitchfx_removed_count", 0)
        setattr(pitch_app_status, "batters_faced_bbref", 0)
        setattr(pitch_app_status, "pitch_count_missing_pitchfx", 0)
        setattr(pitch_app_status, "pitch_count_extra_pitchfx", 0)
        setattr(pitch_app_status, "missing_pitchfx_is_valid", 0)
        setattr(pitch_app_status, "total_at_bats_missing_pitchfx", 0)
        setattr(pitch_app_status, "total_at_bats_extra_pitchfx", 0)
        setattr(pitch_app_status, "total_at_bats_missing_pitchfx", 0)
        db_session.commit()
    return Result.Ok()
