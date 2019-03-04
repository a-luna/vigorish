import time
from pathlib import Path

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

    bbref_game_ids = result.value
    session.commit()

    result = get_all_brooks_dates_scraped(year)
    if result.failure:
        return result
    scraped_brooks_dates = result.value

    result = update_status_brooks_games_for_date(session, scraped_brooks_dates)
    if result.failure:
        return result
    session.commit()

    result_dict = result.value
    brooks_game_ids = result_dict['game_ids']
    might_be_postponed_ids = result_dict['postponed_ids']
    extra_brooks_game_ids = set(brooks_game_ids).difference(set(bbref_game_ids))
    mystery_game_id = set(extra_brooks_game_ids).difference(set(might_be_postponed_ids))
    report = f"""
    Extra Game IDs in Brooks.....: {len(extra_brooks_game_ids)}
    Probably Postponed Game Ids..: {len(might_be_postponed_ids)}
    Mystery Game ID..............: {mystery_game_id}
    """
    print(report)

    result = get_all_bbref_boxscores_scraped(year)
    if result.failure:
        return result
    scraped_bbref_gameids = result.value

    time.sleep(10)
    return Result.Ok()

def update_status_bbref_games_for_date(session, scraped_bbref_dates):
    all_game_ids = []
    for d in tqdm(
        scraped_bbref_dates,
        desc='Updating bbref_games_for_date...',
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
        game_ids = [Path(url).stem for url in games_for_date.boxscore_urls]
        all_game_ids.extend(game_ids)

        setattr(date_status, 'scraped_daily_dash_bbref', 1)
        setattr(date_status, 'game_count_bbref', game_count)
    return Result.Ok(all_game_ids)

def update_status_brooks_games_for_date(session, scraped_brooks_dates):
    all_game_ids = []
    all_might_be_postponed_game_ids = []
    for d in tqdm(
        scraped_brooks_dates,
        desc='Updating brooks_games_for_date..',
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

        game_ids = []
        might_be_postponed_ids = []
        pitch_app_count = 0
        might_be_postponed_count = 0
        for g in games_for_date.games:
            game_ids.append(g.bbref_game_id)
            pitch_app_count += g.pitcher_appearance_count
            if g.might_be_postponed:
                might_be_postponed_ids.append(g.bbref_game_id)
                might_be_postponed_count += 1

        all_game_ids.extend(game_ids)
        all_might_be_postponed_game_ids.extend(might_be_postponed_ids)
        result_dict = dict(
            game_ids=all_game_ids,
            postponed_ids=all_might_be_postponed_game_ids
        )

        setattr(date_status, 'scraped_daily_dash_brooks', 1)
        setattr(date_status, 'game_count_brooks', game_count)
        setattr(date_status, 'pitch_app_count_brooks', pitch_app_count)
        setattr(date_status, 'might_be_postponed_count_brooks', might_be_postponed_count)
    return Result.Ok(result_dict)

#def update_status_bbref_boxscores(session, scraped_game_ids):
    