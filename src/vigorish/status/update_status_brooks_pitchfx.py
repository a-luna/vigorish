from vigorish.config.database import PitchAppScrapeStatus
from vigorish.enums import DataSet
from vigorish.util.result import Result


def update_data_set_brooks_pitchfx(scraped_data, db_session, season):
    scraped_ids = scraped_data.get_all_scraped_pitchfx_pitch_app_ids(season.year)
    unscraped_ids = PitchAppScrapeStatus.get_all_unscraped_pitch_app_ids_for_season(
        db_session, season.id
    )
    new_pitch_app_ids = set(scraped_ids) & set(unscraped_ids)
    if not new_pitch_app_ids:
        return Result.Ok()
    result = update_status_brooks_pitchfx_log_list(scraped_data, db_session, new_pitch_app_ids)
    if result.failure:
        return result
    return Result.Ok()


def update_status_brooks_pitchfx_log_list(scraped_data, db_session, new_pitch_app_ids):
    for pitch_app_id in new_pitch_app_ids:
        pitchfx_log = scraped_data.get_brooks_pitchfx_log(pitch_app_id)
        if not pitchfx_log:
            error = f"Failed to retrieve {DataSet.BROOKS_PITCHFX} (URL ID: {pitch_app_id})"
            return Result.Fail(error)
        result = update_pitch_appearance_status_records(db_session, pitchfx_log)
        if result.failure:
            return result
        db_session.commit()
    return Result.Ok()


def update_pitch_appearance_status_records(db_session, pitchfx_log):
    try:
        pitch_app_status = PitchAppScrapeStatus.find_by_pitch_app_id(
            db_session, pitchfx_log.pitch_app_id
        )
        if not pitch_app_status:
            error = (
                f"scrape_status_pitch_app does not contain an "
                f"entry for pitch_app_id: {pitchfx_log.pitch_app_id}"
            )
            return Result.Fail(error)
        pitch_app_status.scraped_pitchfx = 1
        pitch_app_status.pitch_count_pitchfx = pitchfx_log.total_pitch_count
        return Result.Ok()
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")
