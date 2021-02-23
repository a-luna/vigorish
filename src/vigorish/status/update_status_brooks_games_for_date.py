import vigorish.database as db
from vigorish.enums import DataSet
from vigorish.util.dt_format_strings import DATE_ONLY, DATE_ONLY_TABLE_ID
from vigorish.util.result import Result


def update_brooks_games_for_date_single_date(db_session, season, games_for_date):
    game_date = games_for_date.game_date
    result = update_date_status_records(db_session, games_for_date, game_date)
    if result.failure:
        return result
    result = update_game_status_records(db_session, games_for_date)
    if result.failure:
        return result
    db_session.commit()
    return Result.Ok()


def update_status_brooks_games_for_date_list(scraped_data, db_session, scraped_brooks_dates, apply_patch_list=True):
    season = None
    for game_date in scraped_brooks_dates:
        if not season:
            season = db.Season.find_by_year(db_session, game_date.year)
        games_for_date = scraped_data.get_brooks_games_for_date(game_date, apply_patch_list)
        if not games_for_date:
            date_str = game_date.strftime(DATE_ONLY_TABLE_ID)
            error = f"Failed to retrieve {DataSet.BROOKS_GAMES_FOR_DATE} (URL ID: {date_str})"
            return Result.Fail(error)
        result = update_date_status_records(db_session, games_for_date, game_date)
        if result.failure:
            return result
        result = update_game_status_records(db_session, games_for_date)
        if result.failure:
            return result
        db_session.commit()
    return Result.Ok()


def update_date_status_records(db_session, games_for_date, game_date):
    try:
        date_id = game_date.strftime(DATE_ONLY_TABLE_ID)
        date_status = db_session.query(db.DateScrapeStatus).get(date_id)
        if not date_status:
            date_str = games_for_date.game_date.strftime(DATE_ONLY)
            error = f"scrape_status_date does not contain an entry for date: {date_str}"
            return Result.Fail(error)
        date_status.scraped_daily_dash_brooks = 1
        date_status.game_count_brooks = games_for_date.game_count
        return Result.Ok()
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")


def update_game_status_records(db_session, games_for_date):
    for game_info in games_for_date.games:
        if not game_info.pitcher_appearance_count:
            continue
        try:
            bbref_game_id = game_info.bbref_game_id
            game_status = db.GameScrapeStatus.find_by_bbref_game_id(db_session, bbref_game_id)
            if not game_status:
                game_status = create_game_status_record(db_session, games_for_date, bbref_game_id)
            game_status.bb_game_id = game_info.bb_game_id
            game_status.game_time_hour = game_info.game_time_hour
            game_status.game_time_minute = game_info.game_time_minute
            game_status.game_time_zone = game_info.time_zone_name
            game_status.pitch_app_count_brooks = game_info.pitcher_appearance_count
        except Exception as e:
            return Result.Fail(f"Error: {repr(e)}")
    return Result.Ok()


def create_game_status_record(db_session, games_for_date, bbref_game_id):
    game_date = games_for_date.game_date
    date_status = db.DateScrapeStatus.find_by_date(db_session, game_date)
    date_status.scraped_daily_dash_brooks = 1
    date_status.game_count_brooks = games_for_date.game_count
    game_status = db.GameScrapeStatus()
    game_status.game_date = game_date
    game_status.bbref_game_id = bbref_game_id
    game_status.scrape_status_date_id = date_status.id
    game_status.season_id = date_status.season_id
    db_session.add(game_status)
    return game_status
