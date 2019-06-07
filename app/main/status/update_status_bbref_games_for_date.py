from pathlib import Path

from app.main.models.status_date import DateScrapeStatus
from app.main.models.status_game import GameScrapeStatus
from app.main.util.dt_format_strings import DATE_ONLY, DATE_ONLY_TABLE_ID
from app.main.util.s3_helper import get_all_bbref_dates_scraped, get_bbref_games_for_date_from_s3
from app.main.util.result import Result
from app.main.util.string_functions import validate_bbref_game_id_list


def update_bbref_games_for_date_single_date(session, season, games_for_date):
    game_date = games_for_date.game_date
    unscraped_bbref_dates = DateScrapeStatus.get_all_bbref_unscraped_dates_for_season(session, season.id)
    update_bbref_dates = set([game_date]) & set(unscraped_bbref_dates)
    if not update_bbref_dates:
        return Result.Ok()
    result = update_status_bbref_games_for_date(session, games_for_date)
    if result.failure:
        return result
    new_bbref_game_ids = [Path(url).stem for url in games_for_date.boxscore_urls]
    result = create_game_status_records_from_bbref_ids(session, season, new_bbref_game_ids)
    if result.failure:
        return result
    return Result.Ok()

def update_data_set_bbref_games_for_date(session, season):
    result = get_all_bbref_dates_scraped(season.year)
    if result.failure:
        return result
    scraped_bbref_dates = result.value
    unscraped_bbref_dates = DateScrapeStatus.get_all_bbref_unscraped_dates_for_season(session, season.id)
    update_bbref_dates = set(scraped_bbref_dates) & set(unscraped_bbref_dates)
    if not update_bbref_dates:
        return Result.Ok()
    result = update_status_bbref_games_for_date_list(session, update_bbref_dates)
    if result.failure:
        return result
    new_bbref_game_ids = result.value
    return create_game_status_records_from_bbref_ids(session, season, new_bbref_game_ids)

def update_status_bbref_games_for_date_list(session, scraped_bbref_dates):
    all_game_ids = []
    for game_date in scraped_bbref_dates:
        result = get_bbref_games_for_date_from_s3(game_date)
        if result.failure:
            return result
        games_for_date = result.value
        result = update_status_bbref_games_for_date(session, games_for_date)
        if result.failure:
            return result
        game_ids = [Path(url).stem for url in games_for_date.boxscore_urls]
        all_game_ids.extend(game_ids)
    return Result.Ok(all_game_ids)

def update_status_bbref_games_for_date(session, games_for_date):
    try:
        date_id = games_for_date.game_date.strftime(DATE_ONLY_TABLE_ID)
        date_status = session.query(DateScrapeStatus).get(date_id)
        if not date_status:
            date_str = games_for_date.game_date.strftime(DATE_ONLY)
            error = f'scrape_status_date does not contain an entry for date: {date_str}'
            return Result.Fail(error)
        setattr(date_status, 'scraped_daily_dash_bbref', 1)
        setattr(date_status, 'game_count_bbref', games_for_date.game_count)
        return Result.Ok()
    except Exception as e:
        return Result.Fail(f'Error: {repr(e)}')

def create_game_status_records_from_bbref_ids(session, season, new_bbref_game_ids):
    game_status_bbref_ids = GameScrapeStatus.get_all_bbref_game_ids(session, season.id)
    missing_bbref_game_ids = set(new_bbref_game_ids).difference(set(game_status_bbref_ids))
    if not missing_bbref_game_ids:
        return Result.Ok()
    for game_dict in validate_bbref_game_id_list(missing_bbref_game_ids):
        try:
            bbref_game_id = game_dict['game_id']
            game_date = game_dict['game_date']
            date_status = DateScrapeStatus.find_by_date(session, game_date)
            game_status = GameScrapeStatus()
            setattr(game_status, 'game_date', game_date)
            setattr(game_status, 'bbref_game_id', bbref_game_id)
            setattr(game_status, 'scrape_status_date_id', date_status.id)
            setattr(game_status, 'season_id', season.id)
            session.add(game_status)
        except Exception as e:
            return Result.Fail(f'Error: {repr(e)}')
    return Result.Ok()
