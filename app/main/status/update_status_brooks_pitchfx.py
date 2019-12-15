from app.main.models.season import Season
from app.main.models.status_pitch_appearance import PitchAppearanceScrapeStatus
from app.main.util.s3_helper import get_all_pitch_app_ids_scraped, get_brooks_pitchfx_log_from_s3
from app.main.util.result import Result


def update_data_set_brooks_pitchfx(session, season):
    result = get_all_pitch_app_ids_scraped(season.year)
    if result.failure:
        return result
    scraped_pitch_app_ids = result.value
    scraped_pitch_app_id_dict = PitchAppearanceScrapeStatus.get_pitch_app_id_dict_from_pitch_app_guids(
        session, scraped_pitch_app_ids
    )
    unscraped_bbref_pitch_app_ids = \
        Season.get_unscraped_bbref_pitch_app_ids_for_season(session, season.year)
    new_bbref_pitch_app_ids = set(scraped_pitch_app_id_dict.items()) & set(unscraped_bbref_pitch_app_ids)
    if not new_bbref_pitch_app_ids:
        return Result.Ok()
    result = update_status_brooks_pitchfx_log_list(session, season, new_bbref_pitch_app_ids, scraped_pitch_app_id_dict)
    if result.failure:
        return result
    return Result.Ok()


def update_status_brooks_pitchfx_log_list(session, season, new_bbref_pitch_app_ids, scraped_pitch_app_id_dict):
    for bbref_pitch_app_id in new_bbref_pitch_app_ids:
        pitch_app_id = scraped_pitch_app_id_dict[bbref_pitch_app_id]
        result = get_brooks_pitchfx_log_from_s3(pitch_app_id, year=season.year)
        if result.failure:
            return result
        pitchfx_log = result.value
        result = update_pitch_appearance_status_records(session, pitchfx_log)
        if result.failure:
            return result
    return Result.Ok()


def update_pitch_appearance_status_records(session, pitchfx_log):
    try:
        pitch_app_status = PitchAppearanceScrapeStatus.find_by_bbref_pitch_app_id(session, pitchfx_log.bbref_pitch_app_id)
        if not pitch_app_status:
            error = f'scrape_status_pitch_app does not contain an entry for bbref_pitch_app_id: {pitchfx_log.bbref_pitch_app_id}'
            return Result.Fail(error)
        setattr(pitch_app_status, 'scraped_pitchfx', 1)
        setattr(pitch_app_status, 'pitch_count_pitchfx', pitchfx_log.total_pitch_count)
        return Result.Ok()
    except Exception as e:
        return Result.Fail(f'Error: {repr(e)}')
