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
from app.main.util.string_functions import (
    validate_bbref_game_id,
    validate_bbref_game_id_list,
    validate_bb_game_id
)
from app.main.util.result import Result

def update_status_for_mlb_season(session, year):
    season = Season.find_by_year(session, year)
    if not season:
        error = f'Error occurred retrieving season for year={year}'
        return Result.Fail(error)

    result = update_data_set_bbref_games_for_date(session, season)
    if result.failure:
        return result

    result = update_data_set_brooks_games_for_date(session, season)
    if result.failure:
        return result

    result = update_data_set_bbref_boxscores(session, season)
    if result.failure:
        return result

    result = update_data_set_brooks_pitch_logs(session, season)
    if result.failure:
        return result

    session.commit()
    return Result.Ok()

def update_data_set_bbref_games_for_date(session, season):
    result = get_all_bbref_dates_scraped(season.year)
    if result.failure:
        return result
    scraped_bbref_dates = result.value
    unscraped_bbref_dates = DateScrapeStatus.get_all_bbref_unscraped_dates_for_season(session, season.id)
    update_bbref_dates = set(scraped_bbref_dates) & set(unscraped_bbref_dates)
    if not update_bbref_dates:
        return Result.Ok([])
    result = update_status_bbref_games_for_date_list(session, update_bbref_dates)
    if result.failure:
        return result
    new_bbref_game_ids = result.value
    return create_game_status_records_from_bbref_ids(session, season, new_bbref_game_ids)

def update_status_bbref_games_for_date_list(session, scraped_bbref_dates):
    all_game_ids = []
    with tqdm(
        total=len(scraped_bbref_dates),
        unit="day",
        mininterval=0.12,
        maxinterval=5,
        unit_scale=True,
        position=0
    ) as pbar:
        for game_date in scraped_bbref_dates:
            pbar.set_description("Updating bbref_games_for_date........")
            result = get_bbref_games_for_date_from_s3(game_date)
            if result.failure:
                return result
            games_for_date = result.value
            result = update_status_bbref_games_for_date(session, games_for_date)
            if result.failure:
                return result
            game_ids = [Path(url).stem for url in games_for_date.boxscore_urls]
            all_game_ids.extend(game_ids)
            pbar.update()
    return Result.Ok(all_game_ids)

def update_status_bbref_games_for_date(session, games_for_date):
    try:
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
    except Exception as e:
        return Result.Fail(f'Error: {repr(e)}')

def create_game_status_records_from_bbref_ids(session, season, new_bbref_game_ids):
    game_status_bbref_ids = GameScrapeStatus.get_all_bbref_game_ids(session, season.id)
    missing_bbref_game_ids = set(new_bbref_game_ids).difference(set(game_status_bbref_ids))
    if not missing_bbref_game_ids:
        return Result.Ok()

    with tqdm(
        total=len(missing_bbref_game_ids),
        unit="game",
        mininterval=0.12,
        maxinterval=5,
        unit_scale=True,
        position=1,
        leave=False
    ) as pbar:
        pbar.set_description('Populating scrape_status_game........')
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
                pbar.update()
            except Exception as e:
                return Result.Fail(f'Error: {repr(e)}')
    return Result.Ok()

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
    with tqdm(
        total=len(scraped_brooks_dates),
        unit="day",
        mininterval=0.12,
        maxinterval=5,
        unit_scale=True,
        position=0
    ) as pbar:
        for game_date in scraped_brooks_dates:
            pbar.set_description("Updating brooks_games_for_date.......")
            result = get_brooks_games_for_date_from_s3(game_date)
            if result.failure:
                return result
            games_for_date = result.value
            result = update_status_brooks_games_for_date(session, games_for_date)
            if result.failure:
                return result
            new_brooks_games.extend(games_for_date.games)
            pbar.update()
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
    with tqdm(
        total=len(new_brooks_games),
        unit="game",
        mininterval=0.12,
        maxinterval=5,
        unit_scale=True,
        position=1,
        leave=False
    ) as pbar:
        pbar.set_description('Updating scrape_status_game..........')
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

def update_data_set_bbref_boxscores(session, season):
    result = get_all_bbref_boxscores_scraped(season.year)
    if result.failure:
        return result
    scraped_bbref_gameids = result.value
    unscraped_bbref_gameids = GameScrapeStatus.get_all_unscraped_bbref_game_ids_for_season(session, season.id)
    new_bbref_game_ids = set(scraped_bbref_gameids) & set(unscraped_bbref_gameids)
    if not new_bbref_game_ids:
        return Result.Ok([])
    result = update_status_bbref_boxscore_list(session, new_bbref_game_ids)
    if result.failure:
        return result
    return Result.Ok()

def update_status_bbref_boxscore_list(session, new_bbref_game_ids):
    with tqdm(
        total=len(new_bbref_game_ids),
        unit="game",
        mininterval=0.12,
        maxinterval=5,
        unit_scale=True,
        position=0
    ) as pbar:
        pbar.set_description("Updating bbref_boxscores.............")
        for bbref_game_id in new_bbref_game_ids:
            result = get_bbref_boxscore_from_s3(bbref_game_id)
            if result.failure:
                return result
            boxscore = result.value
            result = update_status_bbref_boxscore(session, boxscore)
            if result.failure:
                return result
            pbar.update()
    return Result.Ok()

def update_status_bbref_boxscore(session, boxscore):
    try:
        game_status = GameScrapeStatus.find_by_bbref_game_id(session, boxscore.bbref_game_id)
        if not game_status:
            error = f'scrape_status_game does not contain an entry for bbref_game_id: {boxscore.bbref_game_id}'
            return Result.Fail(error)
        setattr(game_status, 'scraped_bbref_boxscore', 1)
        setattr(game_status, 'pitch_app_count_bbref', boxscore.get_pitch_appearance_count())
        setattr(game_status, 'total_pitch_count_bbref', boxscore.get_pitch_count())
        return Result.Ok()
    except Exception as e:
        return Result.Fail(f'Error: {repr(e)}')

def update_data_set_brooks_pitch_logs(session, season):
    result = get_all_brooks_pitch_logs_scraped(season.year)
    if result.failure:
        return result
    scraped_brooks_game_ids = result.value
    unscraped_brooks_game_ids = GameScrapeStatus.get_all_unscraped_brooks_game_ids_for_season(session, season.id)
    new_brooks_game_ids = set(scraped_brooks_game_ids) & set(unscraped_brooks_game_ids)
    if not new_brooks_game_ids:
        return Result.Ok()
    result = update_status_brooks_pitch_logs_for_game_list(session, new_brooks_game_ids)
    if result.failure:
        return result
    return Result.Ok()

def update_status_brooks_pitch_logs_for_game_list(session, new_brooks_game_ids):
    with tqdm(
        total=len(new_brooks_game_ids),
        unit="game",
        mininterval=0.12,
        maxinterval=5,
        unit_scale=True,
        position=0
    ) as pbar:
        pbar.set_description("Updating brooks_pitch_logs_for_game..")
        for brooks_game_id in new_brooks_game_ids:
            result = get_brooks_pitch_logs_for_game_from_s3(brooks_game_id)
            if result.failure:
                return result
            pitch_logs_for_game = result.value
            result = update_status_brooks_pitch_logs_for_game(session, pitch_logs_for_game)
            if result.failure:
                return result
            pbar.update()
    return Result.Ok()

def update_status_brooks_pitch_logs_for_game(session, pitch_logs_for_game):
    try:
        bb_game_id = pitch_logs_for_game.bb_game_id
        game_status = GameScrapeStatus.find_by_bb_game_id(session, bb_game_id)
        if not game_status:
            error = f'scrape_status_game does not contain an entry for brooks_game_id: {bb_game_id}'
            return Result.Fail(error)
        total_pitches = sum(log.total_pitch_count for log in pitch_logs_for_game.pitch_logs)
        setattr(game_status, 'scraped_brooks_pitch_logs_for_game', 1)
        setattr(game_status, 'total_pitch_count_brooks', total_pitches)
        return Result.Ok()
    except Exception as e:
        return Result.Fail(f'Error: {repr(e)}')
