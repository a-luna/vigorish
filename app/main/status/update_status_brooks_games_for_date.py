from app.main.models.status_date import DateScrapeStatus
from app.main.models.status_game import GameScrapeStatus
from app.main.util.dt_format_strings import DATE_ONLY, DATE_ONLY_TABLE_ID
from app.main.util.s3_helper import get_all_brooks_dates_scraped, get_brooks_games_for_date_from_s3
from app.main.util.result import Result


def update_data_set_brooks_games_for_date(session, season):
    result = get_all_brooks_dates_scraped(season.year)
    if result.failure:
        return result
    scraped_brooks_dates = result.value
    unscraped_brooks_dates = DateScrapeStatus.get_all_brooks_unscraped_dates_for_season(session, season.id)
    new_brooks_dates = set(scraped_brooks_dates) & set(unscraped_brooks_dates)
    if not new_brooks_dates:
        return Result.Ok()
    result = update_status_brooks_games_for_date_list(session, new_brooks_dates)
    if result.failure:
        return result
    new_brooks_games = result.value
    return update_game_status_records(session, season, new_brooks_games)

def update_status_brooks_games_for_date_list(session, scraped_brooks_dates):
    new_brooks_games = []
    for game_date in scraped_brooks_dates:
        result = get_brooks_games_for_date_from_s3(game_date)
        if result.failure:
            return result
        games_for_date = result.value
        result = update_status_brooks_games_for_date(session, games_for_date)
        if result.failure:
            return result
        new_brooks_games.extend(games_for_date.games)
    return Result.Ok(new_brooks_games)

def update_status_brooks_games_for_date(session, games_for_date):
    try:
        date_id = games_for_date.game_date.strftime(DATE_ONLY_TABLE_ID)
        date_status = session.query(DateScrapeStatus).get(date_id)
        if not date_status:
            date_str = games_for_date.game_date.strftime(DATE_ONLY)
            error = f'scrape_status_date does not contain an entry for date: {date_str}'
            return Result.Fail(error)
        setattr(date_status, 'scraped_daily_dash_brooks', 1)
        setattr(date_status, 'game_count_brooks', games_for_date.game_count)
        return Result.Ok()
    except Exception as e:
        return Result.Fail(f'Error: {repr(e)}')

def update_game_status_records(session, season, new_brooks_games):
    for brooks_game_info in new_brooks_games:
        try:
            bbref_game_id = brooks_game_info.bbref_game_id
            game_date = brooks_game_info.game_start_time
            date_status = DateScrapeStatus.find_by_date(session, game_date)
            game_status = GameScrapeStatus.find_by_bbref_game_id(session, bbref_game_id)
            if not game_status:
                error = f"scrape_status_game does not contain a record for game: {bbref_game_id}"
                return Result.Fail(error)
            setattr(game_status, 'game_date', game_date)
            setattr(game_status, 'bbref_game_id', bbref_game_id)
            setattr(game_status, 'bb_game_id', brooks_game_info.bb_game_id)
            setattr(game_status, 'game_time_hour', brooks_game_info.game_time_hour)
            setattr(game_status, 'game_time_minute', brooks_game_info.game_time_minute)
            setattr(game_status, 'game_time_zone', brooks_game_info.time_zone_name)
            setattr(game_status, 'pitch_app_count_brooks', brooks_game_info.pitcher_appearance_count)
            setattr(game_status, 'scrape_status_date_id', date_status.id)
            setattr(game_status, 'season_id', season.id)
            session.add(game_status)
        except Exception as e:
            return Result.Fail(f'Error: {repr(e)}')
    return Result.Ok()
