import time
from pathlib import Path

from tqdm import tqdm

from app.main.models.season import Season
from app.main.models.status_date import DateScrapeStatus
from app.main.models.status_game import GameScrapeStatus
from app.main.util.dt_format_strings import DATE_ONLY, DATE_ONLY_TABLE_ID
from app.main.util.s3_helper import (
    get_all_bbref_boxscores_scraped, get_all_bbref_dates_scraped,
    get_all_brooks_dates_scraped, get_all_brooks_pitch_logs_scraped,
    get_bbref_boxscore_from_s3, get_bbref_games_for_date_from_s3,
    get_brooks_games_for_date_from_s3, get_brooks_pitch_logs_for_game_from_s3
)
from app.main.util.string_functions import validate_bbref_game_id, validate_bb_game_id
from app.main.util.result import Result

def update_status_for_mlb_season(session, year):
    season = Season.find_by_year(session, year)
    if not season:
        error = f'Error occurred retrieving season for year={year}'
        return Result.Fail(error)

    result = get_all_bbref_dates_scraped(year)
    if result.failure:
        return result
    scraped_bbref_dates = result.value
    unscraped_bbref_dates = DateScrapeStatus.get_all_bbref_unscraped_dates_for_season(session, season.id)

    update_bbref_dates = set(scraped_bbref_dates) & set(unscraped_bbref_dates)
    if update_bbref_dates:
        result = __update_status_all_bbref_games_for_date(
            session,
            update_bbref_dates
        )
        if result.failure:
            return result
        session.commit()
    scraped_bbref_game_ids = DateScrapeStatus.get_all_bbref_scraped_dates_for_season(session, season.id)

    result = get_all_brooks_dates_scraped(year)
    if result.failure:
        return result
    scraped_brooks_dates = result.value
    unscraped_brooks_dates = DateScrapeStatus.get_all_brooks_unscraped_dates_for_season(session, season.id)

    update_brooks_dates = set(scraped_brooks_dates) & set(unscraped_brooks_dates)
    if update_brooks_dates:
        result = __update_status_all_brooks_games_for_date(
            session,
            update_brooks_dates
        )
        if result.failure:
            return result
        session.commit()
        game_id_dict = result.value
        scraped_bbref_game_ids.extend(game_id_dict.keys())

        result = __create_status_records_for_newly_scraped_game_ids(
            session,
            year,
            game_id_dict
        )
        if result.failure:
            return result
        session.commit()

    result = get_all_bbref_boxscores_scraped(year)
    if result.failure:
        return result
    scraped_bbref_gameids = result.value
    unscraped_bbref_gameids = GameScrapeStatus.get_all_unscraped_bbref_game_ids_for_season(session, season.id)
    update_bbref_ids = set(scraped_bbref_gameids) & set(unscraped_bbref_gameids)
    if update_bbref_ids:
        result = __update_status_all_bbref_boxscores(session, update_bbref_ids)
        if result.failure:
            return result
        session.commit()

    result = get_all_brooks_pitch_logs_scraped(year)
    if result.failure:
        return result
    scraped_brooks_gameids = result.value
    unscraped_brooks_gameids = GameScrapeStatus.get_all_unscraped_brooks_game_ids_for_season(session, season.id)
    update_brooks_ids = set(scraped_brooks_gameids) & set(unscraped_brooks_gameids)
    if update_brooks_ids:
        result = __update_status_all_brooks_pitch_logs(session, update_brooks_ids)
        if result.failure:
            return result
        session.commit()

    return Result.Ok()

def __update_status_all_bbref_games_for_date(session, scraped_bbref_dates):
    all_game_ids = []
    for d in tqdm(
        scraped_bbref_dates,
        desc='Updating bbref_games_for_date........',
        ncols=100,
        unit='day',
        mininterval=0.12,
        maxinterval=5,
        unit_scale=True
    ):
        result = get_bbref_games_for_date_from_s3(d)
        if result.failure:
            return result
        games_for_date = result.value

        game_ids = [Path(url).stem for url in games_for_date.boxscore_urls]
        all_game_ids.extend(game_ids)
        result = update_status_bbref_games_for_date(session, games_for_date)
        if result.failure:
            return result
    return Result.Ok(all_game_ids)

def update_status_bbref_games_for_date(session, games_for_date):
    date_id = games_for_date.game_date.strftime(DATE_ONLY_TABLE_ID)
    date_status = session.query(DateScrapeStatus).get(date_id)
    if not date_status:
        date_str = games_for_date.game_date.strftime(DATE_ONLY)
        error = f'scrape_status_date does not contain an entry for date: {date_str}'
        return Result.Fail(error)

    game_count = games_for_date.game_count
    setattr(date_status, 'scraped_daily_dash_bbref', 1)
    setattr(date_status, 'game_count_bbref', game_count)
    return Result.Ok()

def __update_status_all_brooks_games_for_date(session, scraped_brooks_dates):
    game_id_dict = {}
    for d in tqdm(
        scraped_brooks_dates,
        desc='Updating brooks_games_for_date.......',
        ncols=100,
        unit='day',
        mininterval=0.12,
        maxinterval=5,
        unit_scale=True
    ):
        result = get_brooks_games_for_date_from_s3(d)
        if result.failure:
            return result
        games_for_date = result.value
        game_id_dict = games_for_date.get_game_id_dict()
        if not game_id_dict or len(game_id_dict) == 0:
            continue

        result = update_status_brooks_games_for_date(session, games_for_date)
        if result.failure:
            return result
    return Result.Ok(game_id_dict)

def update_status_brooks_games_for_date(session, games_for_date):
    date_id = games_for_date.game_date.strftime(DATE_ONLY_TABLE_ID)
    date_status = session.query(DateScrapeStatus).get(date_id)
    if not date_status:
        date_str = games_for_date.game_date.strftime(DATE_ONLY)
        error = f'scrape_status_date does not contain an entry for date: {date_str}'
        return Result.Fail(error)

    setattr(date_status, 'scraped_daily_dash_brooks', 1)
    setattr(date_status, 'game_count_brooks', games_for_date.game_count)
    return Result.Ok()

def __create_status_records_for_newly_scraped_game_ids(session, year, game_id_dict, disable_pbar=False):
    prev_bbref_game_ids = [
        g.bbref_game_id
        for g
        in session.query(GameScrapeStatus).all()
    ]
    new_bbref_game_ids = set(game_id_dict.keys()).\
        difference(set(prev_bbref_game_ids))
    if not new_bbref_game_ids:
        return Result.Ok()

    season = Season.find_by_year(session, year)
    if not season:
        error = f'Error occurred retrieving season for year={year}'
        return Result.Fail(error)

    for gid in tqdm(
        new_bbref_game_ids,
        desc='Populating scrape_status_game........',
        ncols=100,
        unit='day',
        mininterval=0.12,
        maxinterval=5,
        unit_scale=True,
        disable=disable_pbar
    ):
        try:
            result = validate_bbref_game_id(gid)
            if result.failure:
                return result
            game_dict = result.value
            game_date = game_dict['game_date']
            date_status = DateScrapeStatus.find_by_date(session, game_date)
            game_status = GameScrapeStatus()
            setattr(game_status, 'game_date', game_date)
            setattr(game_status, 'bbref_game_id', gid)
            setattr(game_status, 'bb_game_id', game_id_dict[gid])
            setattr(game_status, 'scrape_status_date_id', date_status.id)
            setattr(game_status, 'season_id', season.id)
            session.add(game_status)
        except Exception as e:
            error = 'Error: {error}'.format(error=repr(e))
            return Result.Fail(error)
    return Result.Ok()

def __update_status_all_bbref_boxscores(session, scraped_bbref_gameids):
    for gid in tqdm(
        scraped_bbref_gameids,
        desc='Updating bbref_boxscores.............',
        ncols=100,
        unit='game',
        mininterval=0.12,
        maxinterval=5,
        unit_scale=True
    ):
        result = get_bbref_boxscore_from_s3(gid)
        if result.failure:
            return result
        boxscore = result.value
        result = __update_status_bbref_boxscore(session, boxscore)
        if result.failure:
            return result
    return Result.Ok()

def __update_status_bbref_boxscore(session, boxscore):
    try:
        game_status = GameScrapeStatus.find_by_bbref_game_id(
            session,
            boxscore.bbref_game_id
        )
        if not game_status:
            error = (
                'scrape_status_game does not contain an entry for '
                f'bbref_game_id: {boxscore.bbref_game_id}'
            )
            return Result.Fail(error)

        away_team_total_pitches = sum(
            pitch_stats.pitch_count
            for pitch_stats
            in boxscore.away_team_data.pitching_stats
        )
        home_team_total_pitches = sum(
            pitch_stats.pitch_count
            for pitch_stats
            in boxscore.home_team_data.pitching_stats
        )
        total_pitches = away_team_total_pitches + home_team_total_pitches
        total_pitch_appearances = len(boxscore.away_team_data.pitching_stats) + \
            len(boxscore.home_team_data.pitching_stats)
        setattr(game_status, 'scraped_bbref_boxscore', 1)
        setattr(game_status, 'pitch_app_count_bbref', total_pitch_appearances)
        setattr(game_status, 'total_pitch_count_bbref', total_pitches)
        return Result.Ok()
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        return Result.Fail(error)

def update_status_bbref_boxscores_list(session, boxscores):
    with tqdm(
        total=len(boxscores),
        ncols=100,
        unit='game',
        mininterval=0.12,
        maxinterval=5,
        leave=False,
        position=1
    ) as pbar:
        for b in boxscores:
            pbar.set_description(f'Updating {b.bbref_game_id}...')
            game_date = b.get_game_date()
            result = __create_status_records_for_newly_scraped_game_ids(
                session,
                game_date.year,
                b.get_game_id_dict()
            )
            if result.failure:
                return result
            result = __update_status_bbref_boxscore(session, b)
            if result.failure:
                return result
            pbar.update()
    return Result.Ok()

def __update_status_all_brooks_pitch_logs(session, scraped_brooks_gameids):
    for gid in tqdm(
        scraped_brooks_gameids,
        desc='Updating brooks_pitch_logs_for_game..',
        ncols=100,
        unit='game',
        mininterval=0.12,
        maxinterval=5,
        unit_scale=True
    ):
        try:
            result = get_brooks_pitch_logs_for_game_from_s3(gid)
            if result.failure:
                return result
            pitch_logs_for_game = result.value
            result = __update_status_brooks_pitch_logs_for_game(
                session,
                pitch_logs_for_game
            )
            if result.failure:
                return result
        except Exception as e:
            error = 'Error: {error}'.format(error=repr(e))
            return Result.Fail(error)
    return Result.Ok()

def __update_status_brooks_pitch_logs_for_game(session, pitch_logs_for_game):
    try:
        result = __get_brooks_game_info_for_game_id(
            pitch_logs_for_game.bb_game_id
        )
        if result.failure:
            return result
        game_info = result.value
        if game_info.might_be_postponed:
            return Result.Ok()
        return __update_db_status_brooks_pitch_logs(session, game_info, pitch_logs_for_game )
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        return Result.Fail(error)

def __get_brooks_game_info_for_game_id(bb_game_id):
    result = validate_bb_game_id(bb_game_id)
    if result.failure:
        return result
    game_dict = result.value
    game_date = game_dict['game_date']
    result = get_brooks_games_for_date_from_s3(game_date)
    if result.failure:
        return result
    games_for_date = result.value
    game = [g
            for g
            in games_for_date.games
            if g.bb_game_id == bb_game_id][0]
    return Result.Ok(game)

def __update_db_status_brooks_pitch_logs(session, game_info, pitch_logs_for_game):
    try:
        game_status = GameScrapeStatus.find_by_bb_game_id(session, game_info.bb_game_id)
        if not game_status:
            error = (
                'scrape_status_game does not contain an entry for '
                f'bb_game_id: {game_info.bb_game_id}'
            )
            return Result.Fail(error)
        total_pitches = sum(log.total_pitch_count for log in pitch_logs_for_game.pitch_logs)
        setattr(game_status, 'scraped_brooks_pitch_logs_for_game', 1)
        setattr(game_status, 'pitch_app_count_brooks', pitch_logs_for_game.pitch_log_count)
        setattr(game_status, 'total_pitch_count_brooks', total_pitches)
        setattr(game_status, 'game_time_hour', game_info.game_time_hour)
        setattr(game_status, 'game_time_minute', game_info.game_time_minute)
        setattr(game_status, 'game_time_zone', game_info.time_zone_name)
        return Result.Ok()
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        return Result.Fail(error)

def update_status_brooks_pitch_logs_for_game_list(session, game_list):
    with tqdm(
        total=len(game_list),
        ncols=100,
        unit='game',
        mininterval=0.12,
        maxinterval=5,
        leave=False,
        position=1
    ) as pbar:
        for logs in game_list:
            pbar.set_description(f'Updating {logs.bbref_game_id}...')
            game_date = logs.get_game_date()
            result = __create_status_records_for_newly_scraped_game_ids(
                session,
                game_date.year,
                logs.get_game_id_dict(),
                disable_pbar=True
            )
            if result.failure:
                return result
            result = __update_status_brooks_pitch_logs_for_game(session, logs)
            if result.failure:
                return result
            pbar.update()
    return Result.Ok()