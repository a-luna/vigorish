from vigorish.status.util import get_date_status, get_game_status
from vigorish.util.dt_format_strings import DATE_ONLY_2
from vigorish.util.result import Result


def update_brooks_games_for_date_list(scraped_data, db_session, scraped_brooks_dates, apply_patch_list=True):
    missing_json = []
    for game_date in sorted(scraped_brooks_dates):
        games_for_date = scraped_data.get_brooks_games_for_date(game_date, apply_patch_list)
        if not games_for_date:
            missing_json.append(game_date.strftime(DATE_ONLY_2))
            continue
        result = update_brooks_games_for_date_single_date(db_session, games_for_date)
        if result.failure:
            return result
    return (
        Result.Ok()
        if not missing_json
        else Result.Fail(f"Failed to retrieve Brooks Games for Date JSON files: {','.join(missing_json)}")
    )


def update_brooks_games_for_date_single_date(db_session, games_for_date):
    game_date = games_for_date.game_date
    result = get_date_status(db_session, game_date)
    if result.failure:
        return result
    date_status = result.value
    update_date_status_record(date_status, games_for_date)
    update_game_status_records(db_session, games_for_date)
    return Result.Ok()


def update_date_status_record(date_status, games_for_date):
    date_status.scraped_daily_dash_brooks = 1
    date_status.game_count_brooks = games_for_date.game_count


def update_game_status_records(db_session, games_for_date):
    for game_info in games_for_date.games:
        game_date = game_info.game_date
        bbref_game_id = game_info.bbref_game_id
        bb_game_id = game_info.bb_game_id
        game_status = get_game_status(db_session, game_date, bbref_game_id, bb_game_id)
        update_game_status_record(game_status, game_info)


def update_game_status_record(game_status, game_info):
    game_status.bb_game_id = game_info.bb_game_id
    game_status.game_time_hour = game_info.game_time_hour
    game_status.game_time_minute = game_info.game_time_minute
    game_status.game_time_zone = game_info.time_zone_name
    game_status.pitch_app_count_brooks = game_info.pitcher_appearance_count
