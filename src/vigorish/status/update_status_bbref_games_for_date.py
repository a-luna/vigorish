from pathlib import Path

from vigorish.config.database import DateScrapeStatus, GameScrapeStatus, Season
from vigorish.enums import DataSet
from vigorish.util.dt_format_strings import DATE_ONLY, DATE_ONLY_TABLE_ID
from vigorish.util.result import Result
from vigorish.util.string_helpers import validate_bbref_game_id_list


def update_bbref_games_for_date_single_date(db_session, season, games_for_date):
    game_date = games_for_date.game_date
    unscraped_bbref_dates = DateScrapeStatus.get_all_bbref_unscraped_dates_for_season(
        db_session, season.id
    )
    update_bbref_dates = set([game_date]) & set(unscraped_bbref_dates)
    if not update_bbref_dates:
        return Result.Ok()
    result = update_status_bbref_games_for_date(db_session, games_for_date)
    if result.failure:
        return result
    new_bbref_game_ids = [Path(url).stem for url in games_for_date.boxscore_urls]
    result = create_game_status_records(db_session, season, new_bbref_game_ids)
    if result.failure:
        return result
    return Result.Ok()


def update_data_set_bbref_games_for_date(scraped_data, db_session, season):
    result = scraped_data.get_all_bbref_dates_scraped(season.year)
    if result.failure:
        return result
    scraped_bbref_dates = result.value
    unscraped_bbref_dates = DateScrapeStatus.get_all_bbref_unscraped_dates_for_season(
        db_session, season.id
    )
    update_bbref_dates = set(scraped_bbref_dates) & set(unscraped_bbref_dates)
    if not update_bbref_dates:
        return Result.Ok()
    result = update_bbref_games_for_date_list(scraped_data, db_session, update_bbref_dates)
    if result.failure:
        return result
    new_bbref_game_ids = result.value
    return create_game_status_records(db_session, season, new_bbref_game_ids)


def update_bbref_games_for_date_list(scraped_data, db_session, scraped_bbref_dates):
    season = None
    for game_date in scraped_bbref_dates:
        if not season:
            season = Season.find_by_year(db_session, game_date.year)
        games_for_date = scraped_data.get_bbref_games_for_date(game_date)
        if not games_for_date:
            date_str = game_date.strftime(DATE_ONLY_TABLE_ID)
            error = f"Failed to retrieve {DataSet.BBREF_GAMES_FOR_DATE} (URL ID: {date_str})"
            return Result.Fail(error)
        result = update_status_bbref_games_for_date(db_session, games_for_date)
        if result.failure:
            return result
        game_ids = [Path(url).stem for url in games_for_date.boxscore_urls]
        result = create_game_status_records(db_session, season, game_ids)
        if result.failure:
            return result
        db_session.commit()
    return Result.Ok()


def update_status_bbref_games_for_date(db_session, games_for_date):
    try:
        date_id = games_for_date.game_date.strftime(DATE_ONLY_TABLE_ID)
        date_status = db_session.query(DateScrapeStatus).get(date_id)
        if not date_status:
            date_str = games_for_date.game_date.strftime(DATE_ONLY)
            error = f"scrape_status_date does not contain an entry for date: {date_str}"
            return Result.Fail(error)
        date_status.scraped_daily_dash_bbref = 1
        date_status.game_count_bbref = games_for_date.game_count
        return Result.Ok()
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")


def create_game_status_records(db_session, season, new_bbref_game_ids):
    game_status_bbref_ids = GameScrapeStatus.get_all_bbref_game_ids(db_session, season.id)
    missing_bbref_game_ids = set(new_bbref_game_ids).difference(set(game_status_bbref_ids))
    if not missing_bbref_game_ids:
        return Result.Ok()
    for game_dict in validate_bbref_game_id_list(missing_bbref_game_ids):
        try:
            game_date = game_dict["game_date"]
            date_status = DateScrapeStatus.find_by_date(db_session, game_date)
            game_status = GameScrapeStatus()
            game_status.game_date = game_date
            game_status.bbref_game_id = game_dict["game_id"]
            game_status.scrape_status_date_id = date_status.id
            game_status.season_id = season.id
            db_session.add(game_status)
        except Exception as e:
            return Result.Fail(f"Error: {repr(e)}")
    return Result.Ok()
