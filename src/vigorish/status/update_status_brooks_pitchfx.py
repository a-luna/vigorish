import vigorish.database as db
from vigorish.util.result import Result


def update_status_brooks_pitchfx_log_list(scraped_data, db_session, new_pitch_app_ids, apply_patch_list=True):
    pfx_missing_json, pfx_missing_db_records = [], []
    for pitch_app_id in new_pitch_app_ids:
        pitchfx_log = scraped_data.get_brooks_pitchfx_log(pitch_app_id, apply_patch_list)
        if not pitchfx_log:
            pfx_missing_json.append(pitch_app_id)
            continue
        result = update_pitch_appearance_status_records(db_session, pitchfx_log)
        if result.failure:
            pfx_missing_db_records.append(pitch_app_id)
            continue
        db_session.commit()
    return (
        Result.Ok()
        if not (pfx_missing_json or pfx_missing_db_records)
        else Result.Fail(_get_error_message(pfx_missing_json, pfx_missing_db_records))
    )


def _get_error_message(pfx_missing_json, pfx_missing_db_records):
    errors = []
    if pfx_missing_json:
        errors.append(f"Failed to retrieve JSON files for PitchFX logs: {','.join(pfx_missing_json)}")
    if pfx_missing_db_records:
        errors.append(f"pitch_app_id not found in scrape_status_pitch_app table: {','.join(pfx_missing_db_records)}")
    return "\n".join(errors)


def update_pitch_appearance_status_records(db_session, pitchfx_log):
    try:
        pitch_app_status = db.PitchAppScrapeStatus.find_by_pitch_app_id(db_session, pitchfx_log.pitch_app_id)
        if not pitch_app_status:
            return Result.Fail()
        pitch_app_status.scraped_pitchfx = 1
        pitch_app_status.pitch_count_pitchfx = pitchfx_log.total_pitch_count
        return Result.Ok()
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")
