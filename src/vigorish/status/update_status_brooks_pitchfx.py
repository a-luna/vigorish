from vigorish.config.database import PitchAppearanceScrapeStatus
from vigorish.util.result import Result


def update_data_set_brooks_pitchfx(scraped_data, session, season):
    result = scraped_data.get_all_scraped_pitchfx_pitch_app_ids(season.year)
    if result.failure:
        return result
    scraped_pitch_app_ids = result.value
    unscraped_pitch_app_ids = PitchAppearanceScrapeStatus.get_all_unscraped_pitch_app_ids_for_season(
        session, season.id
    )
    new_pitch_app_ids = set(scraped_pitch_app_ids) & set(unscraped_pitch_app_ids)
    if not new_pitch_app_ids:
        return Result.Ok()
    result = update_status_brooks_pitchfx_log_list(scraped_data, session, new_pitch_app_ids)
    if result.failure:
        return result
    return Result.Ok()


def update_status_brooks_pitchfx_log_list(scraped_data, session, new_pitch_app_ids):
    for pitch_app_id in new_pitch_app_ids:
        result = scraped_data.get_brooks_pitchfx_log(pitch_app_id)
        if result.failure:
            return result
        pitchfx_log = result.value
        result = update_pitch_appearance_status_records(session, pitchfx_log)
        if result.failure:
            return result
    return Result.Ok()


def update_pitch_appearance_status_records(session, pitchfx_log):
    try:
        pitch_app_status = PitchAppearanceScrapeStatus.find_by_pitch_app_id(
            session, pitchfx_log.pitch_app_id
        )
        if not pitch_app_status:
            error = f"scrape_status_pitch_app does not contain an entry for pitch_app_id: {pitchfx_log.pitch_app_id}"
            return Result.Fail(error)
        setattr(pitch_app_status, "scraped_pitchfx", 1)
        setattr(pitch_app_status, "pitch_count_pitchfx", pitchfx_log.total_pitch_count)
        return Result.Ok()
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")
