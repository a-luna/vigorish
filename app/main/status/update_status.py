import time

from tqdm import tqdm

from app.main.models.season import Season
from app.main.models.status_date import DateScrapeStatus
from app.main.util.dt_format_strings import DATE_ONLY, DATE_ONLY_TABLE_ID
from app.main.util.s3_helper import (
    get_all_bbref_boxscores_scraped, get_all_bbref_dates_scraped,
    get_all_brooks_dates_scraped, get_bbref_boxscore_from_s3,
    get_bbref_games_for_date_from_s3, get_brooks_games_for_date_from_s3
)
from app.main.util.result import Result

def update_status_for_mlb_season(session, year):
    result = get_all_bbref_dates_scraped(year)
    if result.failure:
        return result
    scraped_bbref_dates = result.value

    result = update_status_bbref_games_for_date(session, scraped_bbref_dates)
    if result.failure:
        return result
    session.commit()

    result = get_all_brooks_dates_scraped(year)
    if result.failure:
        return result
    scraped_brooks_dates = result.value

    result = update_status_brooks_games_for_date(session, scraped_brooks_dates)
    if result.failure:
        return result
    session.commit()

    result = get_all_bbref_boxscores_scraped(year)
    if result.failure:
        return result
    scraped_bbref_gameids = result.value

    time.sleep(10)
    return Result.Ok()

def update_status_bbref_games_for_date(session, scraped_bbref_dates):
    for d in tqdm(
        scraped_bbref_dates,
        desc='Updating bbref_games_for_date',
        ncols=100,
        unit='day',
        mininterval=0.12,
        maxinterval=5,
        unit_scale=True
    ):
        date_id = d.strftime(DATE_ONLY_TABLE_ID)
        date_status = session.query(DateScrapeStatus).get(date_id)
        if not date_status:
            date_str = d.strftime(DATE_ONLY)
            error = f'scrape_status_date does not contain an entry for date: {date_str}'
            return Result.Fail(error)

        result = get_bbref_games_for_date_from_s3(d)
        if result.failure:
            return result
        games_for_date = result.value
        game_count = games_for_date.game_count

        setattr(date_status, 'scraped_daily_dash_bbref', 1)
        setattr(date_status, 'game_count_bbref', game_count)
    return Result.Ok()

def update_status_brooks_games_for_date(session, scraped_brooks_dates):
    for d in tqdm(
        scraped_brooks_dates,
        desc='Updating bbref_games_for_date',
        ncols=100,
        unit='day',
        mininterval=0.12,
        maxinterval=5,
        unit_scale=True
    ):
        date_id = d.strftime(DATE_ONLY_TABLE_ID)
        date_status = session.query(DateScrapeStatus).get(date_id)
        if not date_status:
            date_str = d.strftime(DATE_ONLY)
            error = f'scrape_status_date does not contain an entry for date: {date_str}'
            return Result.Fail(error)

        result = get_brooks_games_for_date_from_s3(d)
        if result.failure:
            return result
        games_for_date = result.value
        game_count = games_for_date.game_count

        pitch_app_count = 0
        for g in games_for_date.games:
            pitch_app_count += g.pitcher_appearance_count

        setattr(date_status, 'scraped_daily_dash_brooks', 1)
        setattr(date_status, 'game_count_brooks', game_count)
        setattr(date_status, 'pitch_app_count_brooks', pitch_app_count)
    return Result.Ok()

#def update_status_bbref_boxscores(session, scraped_game_ids):
    