from vigorish.config.database import PitchAppScrapeStatus
from vigorish.util.list_helpers import flatten_list2d
from vigorish.util.result import Result
from vigorish.util.string_helpers import validate_at_bat_id


def update_pitch_apps_for_game_audit_successful(db_session, scraped_data, bbref_game_id):
    combined_data = scraped_data.get_json_combined_data(bbref_game_id)
    if not combined_data:
        return Result.Fail(f"Failed to retrieve combined data file for {bbref_game_id}")
    away_team_pitch_stats = combined_data["away_team_data"]["pitching_stats"]
    home_team_pitch_stats = combined_data["home_team_data"]["pitching_stats"]
    pitch_stats = away_team_pitch_stats + home_team_pitch_stats
    pitch_stats_dict = {pa["pitch_app_id"]: pa for pa in pitch_stats}
    update_pitch_app_ids = [pitch_app_id for pitch_app_id in pitch_stats_dict.keys()]
    miss_pfx_at_bat_ids = combined_data["pitchfx_vs_bbref_audit"]["at_bat_ids_missing_pitchfx"]
    if miss_pfx_at_bat_ids:
        miss_pfx_ab_dicts = [validate_at_bat_id(ab_id).value for ab_id in miss_pfx_at_bat_ids]
        miss_pfx_pitch_app_ids = list(set(ab["pitch_app_id"] for ab in miss_pfx_ab_dicts))
        for pitch_app_id in miss_pfx_pitch_app_ids:
            result = get_pitch_app_status(db_session, pitch_app_id)
            if result.failure:
                return result
            pitch_app_status = result.value
            pitch_stats = pitch_stats_dict[pitch_app_id]
            update_pitch_app_missing_pfx(pitch_app_status, pitch_stats, combined_data)
            db_session.commit()
        update_pitch_app_ids = list(set(update_pitch_app_ids) - set(miss_pfx_pitch_app_ids))
    for pitch_app_id in update_pitch_app_ids:
        result = get_pitch_app_status(db_session, pitch_app_id)
        if result.failure:
            return result
        pitch_app_status = result.value
        pitch_stats = pitch_stats_dict[pitch_app_id]
        update_pitch_app_pfx_complete(pitch_app_status, pitch_stats)
        db_session.commit()
    return Result.Ok()


def get_pitch_app_status(db_session, pitch_app_id):
    pitch_app_status = PitchAppScrapeStatus.find_by_pitch_app_id(db_session, pitch_app_id)
    if not pitch_app_status:
        error = (
            f"scrape_status_pitch_app does not contain an "
            f"entry for pitch_app_id: {pitch_app_id}"
        )
        return Result.Fail(error)
    return Result.Ok(pitch_app_status)


def update_pitch_app_missing_pfx(pitch_app_status, pitch_stats, combined_data):
    missing_pfx_count = (
        pitch_stats["total_pitch_count_bbref"] - pitch_stats["total_pitch_count_pitchfx"]
    )
    at_bat_data_missing_pfx = get_at_bat_data_missing_pfx_for_pitch_app(
        combined_data, pitch_stats["pitch_app_id"]
    )
    missing_pitchfx_is_valid = (
        1
        if all(at_bat_data["missing_pitchfx_is_valid"] for at_bat_data in at_bat_data_missing_pfx)
        else 0
    )
    total_at_bats_pitchfx_complete = pitch_stats["batters_faced_pitchfx"] - len(
        at_bat_data_missing_pfx
    )
    setattr(pitch_app_status, "pitchfx_data_complete", 0)
    setattr(pitch_app_status, "pitch_count_missing_pitchfx", missing_pfx_count)
    setattr(
        pitch_app_status, "missing_pitchfx_is_valid", missing_pitchfx_is_valid,
    )
    setattr(pitch_app_status, "total_at_bats_pitchfx_complete", total_at_bats_pitchfx_complete)
    setattr(pitch_app_status, "total_at_bats_missing_pitchfx", len(at_bat_data_missing_pfx))
    update_pitch_app_combined_data(pitch_app_status, pitch_stats)


def get_at_bat_data_missing_pfx_for_pitch_app(combined_data, pitch_app_id):
    miss_pfx_at_bat_ids = combined_data["pitchfx_vs_bbref_audit"]["at_bat_ids_missing_pitchfx"]
    at_bat_dicts = [validate_at_bat_id(ab_id).value for ab_id in miss_pfx_at_bat_ids]
    pbp_data = combined_data["play_by_play_data"]
    all_inning_events = flatten_list2d([pbp["inning_events"] for pbp in pbp_data])
    miss_pfx_at_bat_data = {
        event["at_bat_id"]: event
        for event in all_inning_events
        if event["at_bat_id"] in miss_pfx_at_bat_ids
    }
    miss_pfx_at_bat_ids_for_pitch_app = [
        at_bat_dict["at_bat_id"]
        for at_bat_dict in at_bat_dicts
        if at_bat_dict["pitch_app_id"] == pitch_app_id
    ]
    return [miss_pfx_at_bat_data[at_bat_id] for at_bat_id in miss_pfx_at_bat_ids_for_pitch_app]


def update_pitch_app_pfx_complete(pitch_app_status, pitch_stats):
    setattr(pitch_app_status, "pitchfx_data_complete", 1)
    setattr(pitch_app_status, "pitch_count_missing_pitchfx", 0)
    setattr(pitch_app_status, "missing_pitchfx_is_valid", 1)
    setattr(
        pitch_app_status, "total_at_bats_pitchfx_complete", pitch_stats["batters_faced_pitchfx"]
    )
    setattr(pitch_app_status, "total_at_bats_missing_pitchfx", 0)
    update_pitch_app_combined_data(pitch_app_status, pitch_stats)


def update_pitch_app_combined_data(pitch_app_status, pitch_stats):
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
    setattr(pitch_app_status, "batters_faced_bbref", pitch_stats["batters_faced_bbref"])


def update_pitch_appearances_audit_failed(db_session, pitch_app_ids):
    for pitch_app_id in pitch_app_ids:
        result = get_pitch_app_status(db_session, pitch_app_id)
        if result.failure:
            return result
        pitch_app_status = result.value
        setattr(pitch_app_status, "audit_failed", 1)
        db_session.commit()
    return Result.Ok()
