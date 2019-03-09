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
    result = get_all_bbref_dates_scraped(year)
    if result.failure:
        return result
    scraped_bbref_dates = result.value

    result = update_status_bbref_games_for_date(session, scraped_bbref_dates)
    if result.failure:
        return result
    session.commit()
    scraped_bbref_game_ids = result.value


    result = get_all_brooks_dates_scraped(year)
    if result.failure:
        return result
    scraped_brooks_dates = result.value

    result = update_status_brooks_games_for_date(session, scraped_brooks_dates)
    if result.failure:
        return result
    session.commit()
    scraped_bbref_game_ids.extend(result.value)

    result = create_status_records_for_newly_scraped_game_ids(
        session,
        year,
        scraped_bbref_game_ids
    )
    if result.failure:
        return result
    session.commit()

    result = get_all_bbref_boxscores_scraped(year)
    if result.failure:
        return result
    scraped_bbref_gameids = result.value

    result = update_status_bbref_boxscores(session, scraped_bbref_gameids)
    if result.failure:
        return result
    session.commit()

    result = get_all_brooks_pitch_logs_scraped(year)
    if result.failure:
        return result
    scraped_brooks_gameids = result.value

    result = update_status_brooks_pitch_logs(session, scraped_brooks_gameids)
    if result.failure:
        return result
    session.commit()

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
        game_ids = [g.bbref_game_id
                    for g
                    in games_for_date.games
                    if not g.might_be_postponed]
        game_count = len(game_ids)
        all_game_ids.extend(game_ids)

        setattr(date_status, 'scraped_daily_dash_brooks', 1)
        setattr(date_status, 'game_count_brooks', game_count)
    return Result.Ok(all_game_ids)

def create_status_records_for_newly_scraped_game_ids(session, year, scraped_bbref_game_ids):
    prev_bbref_game_ids = [
        g.bbref_game_id
        for g
        in session.query(GameScrapeStatus).all()
    ]
    new_bbref_game_ids = set(scraped_bbref_game_ids).\
        difference(set(prev_bbref_game_ids))
    if not new_bbref_game_ids:
        return Result.Ok()

    season = Season.find_by_year(session, year)
    if not season:
        error = f'Error occurred retrieving season for year={year}'
        return Result.Fail(error)

    for gid in tqdm(
        new_bbref_game_ids,
        desc='Populating scrape_status_game..',
        ncols=100,
        unit='day',
        mininterval=0.12,
        maxinterval=5,
        unit_scale=True
    ):
        try:
            result = validate_bbref_game_id(gid)
            if result.failure:
                return result
            game_dict = result.value
            game_date = game_dict['game_date']
            date_status = DateScrapeStatus.find_by_date(session, game_date)
            game_scrape_status = GameScrapeStatus()
            setattr(game_scrape_status, 'bbref_game_id', gid)
            setattr(game_scrape_status, 'game_date', game_date)
            setattr(game_scrape_status, 'scrape_status_date_id', date_status.id)
            setattr(game_scrape_status, 'season_id', season.id)
            session.add(game_scrape_status)
        except Exception as e:
            error = 'Error: {error}'.format(error=repr(e))
            return Result.Fail(error)
    return Result.Ok()

def update_status_bbref_boxscores(session, scraped_bbref_gameids):
    for gid in tqdm(
        scraped_bbref_gameids,
        desc='Updating bbref_boxscores..',
        ncols=100,
        unit='day',
        mininterval=0.12,
        maxinterval=5,
        unit_scale=True
    ):
        try:
            game_status = GameScrapeStatus.find_by_bbref_game_id(session, gid)
            if not game_status:
                error = (
                    'scrape_status_game does not contain an entry for '
                    f'bbref_game_id: {gid}'
                )
                return Result.Fail(error)

            result = get_bbref_boxscore_from_s3(gid)
            if result.failure:
                return result
            boxscore = result.value
            away_team_pitch_stats = boxscore.away_team_data.pitching_stats
            home_team_pitch_stats = boxscore.home_team_data.pitching_stats
            away_team_total_pitches = sum(pitch_stats.pitch_count for pitch_stats in away_team_pitch_stats)
            home_team_total_pitches = sum(pitch_stats.pitch_count for pitch_stats in home_team_pitch_stats)
            total_pitches = away_team_total_pitches + home_team_total_pitches
            total_pitch_appearances = len(away_team_pitch_stats) + len(home_team_pitch_stats)
            setattr(game_status, 'scraped_bbref_boxscore', 1)
            setattr(game_status, 'pitch_app_count_bbref', total_pitch_appearances)
            setattr(game_status, 'total_pitch_count_bbref', total_pitches)
        except Exception as e:
            error = 'Error: {error}'.format(error=repr(e))
            return Result.Fail(error)
    return Result.Ok()


def update_status_brooks_pitch_logs(session, scraped_brooks_gameids):
    for gid in tqdm(
        scraped_brooks_gameids,
        desc='Updating brooks_pitch_logs_for_game..',
        ncols=100,
        unit='day',
        mininterval=0.12,
        maxinterval=5,
        unit_scale=True
    ):
        try:
            result = validate_bb_game_id(gid)
            if result.failure:
                return result
            game_dict = result.value
            game_date = game_dict['game_date']

            result = get_brooks_games_for_date_from_s3(game_date)
            if result.failure:
                return result
            games_for_date = result.value
            this_game = [g for g in games_for_date.games if g.bb_game_id == gid][0]

            game_status = GameScrapeStatus.find_by_bbref_game_id(session, this_game.bbref_game_id)
            if not game_status:
                error = (
                    'scrape_status_game does not contain an entry for '
                    f'bbref_game_id: {gid}'
                )
                return Result.Fail(error)

            result = get_all_brooks_pitch_logs_scraped(gid)
            if result.failure:
                return result
            pitch_logs_for_game = result.value
            total_pitches = sum(log.total_pitch_count for log in pitch_logs_for_game.pitch_logs)
            setattr(game_status, 'scraped_brooks_pitch_logs_for_game', 1)
            setattr(game_status, 'pitch_app_count_brooks', pitch_logs_for_game.pitch_log_count)
            setattr(game_status, 'total_pitch_count_brooks', total_pitches)
            setattr(game_status, 'bb_game_id', gid)
            setattr(game_status, 'game_time_hour', this_game.game_time_hour)
            setattr(game_status, 'game_time_minute', this_game.game_time_minute)
            setattr(game_status, 'game_time_zone', this_game.time_zone_name)
        except Exception as e:
            error = 'Error: {error}'.format(error=repr(e))
            return Result.Fail(error)
    return Result.Ok()