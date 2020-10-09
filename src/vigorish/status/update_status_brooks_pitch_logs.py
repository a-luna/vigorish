from vigorish.config.database import (
    DateScrapeStatus,
    GameScrapeStatus,
    PitchAppScrapeStatus,
    Season,
)
from vigorish.enums import DataSet
from vigorish.util.result import Result


def update_data_set_brooks_pitch_logs(scraped_data, db_session, season):
    result = scraped_data.get_all_scraped_brooks_game_ids(season.year)
    if result.failure:
        return result
    scraped_brooks_game_ids = result.value
    unscraped_brooks_game_ids = GameScrapeStatus.get_all_unscraped_brooks_game_ids_for_season(
        db_session, season.id
    )
    new_brooks_game_ids = set(scraped_brooks_game_ids) & set(unscraped_brooks_game_ids)
    if not new_brooks_game_ids:
        return Result.Ok()
    result = update_status_brooks_pitch_logs_for_game_list(
        scraped_data, db_session, season, new_brooks_game_ids
    )
    if result.failure:
        return result
    return Result.Ok()


def update_status_brooks_pitch_logs_for_game_list(scraped_data, db_session, new_brooks_game_ids):
    for bb_game_id in new_brooks_game_ids:
        pitch_logs_for_game = scraped_data.get_brooks_pitch_logs_for_game(bb_game_id)
        if not pitch_logs_for_game:
            error = f"Failed to retrieve {DataSet.BROOKS_PITCH_LOGS} (URL ID: {bb_game_id})"
            return Result.Fail(error)
        result = update_game_status_records(db_session, pitch_logs_for_game)
        if result.failure:
            return result
        result = create_pitch_appearance_status_records(db_session, pitch_logs_for_game)
        if result.failure:
            return result
        db_session.commit()
    return Result.Ok()


def update_status_brooks_pitch_logs_for_game(db_session, pitch_logs_for_game):
    result = update_game_status_records(db_session, pitch_logs_for_game)
    if result.failure:
        return result
    return create_pitch_appearance_status_records(db_session, pitch_logs_for_game)


def update_game_status_records(db_session, pitch_logs_for_game):
    try:
        bb_game_id = pitch_logs_for_game.bb_game_id
        game_status = GameScrapeStatus.find_by_bb_game_id(db_session, bb_game_id)
        if not game_status:
            error = f"scrape_status_game does not contain an entry for bb_game_id: {bb_game_id}"
            return Result.Fail(error)
        game_status.scraped_brooks_pitch_logs = 1
        return Result.Ok()
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")


def create_pitch_appearance_status_records(db_session, pitch_logs_for_game):
    season = None
    bbref_game_id = pitch_logs_for_game.bbref_game_id
    all_pitch_app_ids = PitchAppScrapeStatus.get_all_pitch_app_ids_for_game(
        db_session, bbref_game_id
    )
    scraped_pitch_app_ids = [plog.pitch_app_id for plog in pitch_logs_for_game.pitch_logs]
    update_pitch_app_ids = set(scraped_pitch_app_ids).difference(all_pitch_app_ids)
    for pitch_log in pitch_logs_for_game.pitch_logs:
        if pitch_log.pitch_app_id not in update_pitch_app_ids:
            continue
        if not season:
            season = Season.find_by_year(db_session, pitch_log.game_date.year)
        try:
            date_status = DateScrapeStatus.find_by_date(db_session, pitch_log.game_date)
            game_status = GameScrapeStatus.find_by_bb_game_id(db_session, pitch_log.bb_game_id)
            pitch_app_status = PitchAppScrapeStatus()
            pitch_app_status.pitcher_id_mlb = pitch_log.pitcher_id_mlb
            pitch_app_status.pitch_app_id = pitch_log.pitch_app_id
            pitch_app_status.bbref_game_id = pitch_log.bbref_game_id
            pitch_app_status.bb_game_id = pitch_log.bb_game_id
            pitch_app_status.pitch_count_pitch_log = pitch_log.total_pitch_count
            pitch_app_status.scrape_status_game_id = game_status.id
            pitch_app_status.scrape_status_date_id = date_status.id
            pitch_app_status.season_id = season.id
            if not pitch_log.parsed_all_info:
                pitch_app_status.scraped_pitchfx = 1
                pitch_app_status.no_pitchfx_data = 1
            db_session.add(pitch_app_status)
        except Exception as e:
            return Result.Fail(f"Error: {repr(e)}")
    return Result.Ok()
