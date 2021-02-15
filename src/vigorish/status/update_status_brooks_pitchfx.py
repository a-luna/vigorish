import vigorish.database as db
from vigorish.util.result import Result


def update_status_brooks_pitchfx_log_list(scraped_data, db_session, new_pitch_app_ids, apply_patch_list=False):
    errors = []
    for pitch_app_id in new_pitch_app_ids:
        pitchfx_log = scraped_data.get_brooks_pitchfx_log(pitch_app_id)
        if not pitchfx_log:
            errors.append(pitch_app_id)
            continue
        result = update_pitch_appearance_status_records(db_session, pitchfx_log)
        if result.failure:
            return result
        db_session.commit()
    return Result.Ok() if not errors else Result.Fail(",".join(errors))


def update_pitch_appearance_status_records(db_session, pitchfx_log):
    try:
        pitch_app_status = db.PitchAppScrapeStatus.find_by_pitch_app_id(db_session, pitchfx_log.pitch_app_id)
        if not pitch_app_status:
            error = f"scrape_status_pitch_app does not contain an entry for pitch_app_id: {pitchfx_log.pitch_app_id}"
            return Result.Fail(error)
        pitch_app_status.scraped_pitchfx = 1
        pitch_app_status.pitch_count_pitchfx = pitchfx_log.total_pitch_count
        return Result.Ok()
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")
