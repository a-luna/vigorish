from vigorish.config.database import DateScrapeStatus, GameScrapeStatus, Season
from vigorish.enums import DataSet
from vigorish.util.dt_format_strings import DATE_ONLY, DATE_ONLY_TABLE_ID
from vigorish.util.result import Result


def update_brooks_games_for_date_single_date(db_session, season, games_for_date):
    game_date = games_for_date.game_date
    result = update_date_status_records(db_session, games_for_date, game_date)
    if result.failure:
        return result
    result = update_game_status_records(db_session, season, games_for_date.games)
    if result.failure:
        return result
    return Result.Ok()


def update_data_set_brooks_games_for_date(scraped_data, db_session, season):
    result = scraped_data.get_all_brooks_dates_scraped(season.year)
    if result.failure:
        return result
    scraped_brooks_dates = result.value
    unscraped_brooks_dates = DateScrapeStatus.get_all_brooks_unscraped_dates_for_season(
        db_session, season.id
    )
    new_brooks_dates = set(scraped_brooks_dates) & set(unscraped_brooks_dates)
    if not new_brooks_dates:
        return Result.Ok()
    result = update_status_brooks_games_for_date_list(scraped_data, db_session, new_brooks_dates)
    if result.failure:
        return result
    new_brooks_games = result.value
    return update_game_status_records(db_session, season, new_brooks_games)


def update_status_brooks_games_for_date_list(scraped_data, db_session, scraped_brooks_dates):
    season = None
    for game_date in scraped_brooks_dates:
        if not season:
            season = Season.find_by_year(db_session, game_date.year)
        games_for_date = scraped_data.get_brooks_games_for_date(game_date)
        if not games_for_date:
            date_str = game_date.strftime(DATE_ONLY_TABLE_ID)
            error = f"Failed to retrieve {DataSet.BROOKS_GAMES_FOR_DATE} (URL ID: {date_str})"
            return Result.Fail(error)
        result = update_date_status_records(db_session, games_for_date, game_date)
        if result.failure:
            return result
        result = update_game_status_records(db_session, season, games_for_date.games)
        if result.failure:
            return result
        db_session.commit()
    return Result.Ok()


def update_date_status_records(db_session, games_for_date, game_date):
    try:
        date_id = game_date.strftime(DATE_ONLY_TABLE_ID)
        date_status = db_session.query(DateScrapeStatus).get(date_id)
        if not date_status:
            date_str = games_for_date.game_date.strftime(DATE_ONLY)
            error = f"scrape_status_date does not contain an entry for date: {date_str}"
            return Result.Fail(error)
        date_status.scraped_daily_dash_brooks = 1
        date_status.game_count_brooks = games_for_date.game_count
        return Result.Ok()
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")


def update_game_status_records(db_session, season, new_brooks_games):
    for brooks_game_info in new_brooks_games:
        if not brooks_game_info.pitcher_appearance_count:
            continue
        try:
            bbref_game_id = brooks_game_info.bbref_game_id
            game_date = brooks_game_info.game_date
            date_status = DateScrapeStatus.find_by_date(db_session, game_date)
            game_status = GameScrapeStatus.find_by_bbref_game_id(db_session, bbref_game_id)
            if not game_status:
                error = f"scrape_status_game does not contain a record for game: {bbref_game_id}"
                return Result.Fail(error)
            game_status.game_date = game_date
            game_status.bbref_game_id = bbref_game_id
            game_status.bb_game_id = brooks_game_info.bb_game_id
            game_status.game_time_hour = brooks_game_info.game_time_hour
            game_status.game_time_minute = brooks_game_info.game_time_minute
            game_status.game_time_zone = brooks_game_info.time_zone_name
            game_status.pitch_app_count_brooks = brooks_game_info.pitcher_appearance_count
            game_status.scrape_status_date_id = date_status.id
            game_status.season_id = season.id
            db_session.add(game_status)
        except Exception as e:
            return Result.Fail(f"Error: {repr(e)}")
    return Result.Ok()
