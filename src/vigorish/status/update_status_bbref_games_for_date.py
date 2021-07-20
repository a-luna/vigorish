from vigorish.status.util import get_date_status, get_date_status_from_bbref_game_id, get_game_status
from vigorish.util.dt_format_strings import DATE_ONLY_2
from vigorish.util.result import Result


def update_bbref_games_for_date_list(scraped_data, db_session, scraped_bbref_dates, apply_patch_list=True):
    missing_json = []
    for game_date in sorted(scraped_bbref_dates):
        games_for_date = scraped_data.get_bbref_games_for_date(game_date, apply_patch_list)
        if not games_for_date:
            missing_json.append(game_date.strftime(DATE_ONLY_2))
            continue
        result = update_bbref_games_for_date_single_date(db_session, games_for_date)
        if result.failure:
            return result
    return (
        Result.Ok()
        if not missing_json
        else Result.Fail(f"Failed to retrieve BBRef Games for Date JSON files: {','.join(missing_json)}")
    )


def update_bbref_games_for_date_single_date(db_session, games_for_date):
    game_date = games_for_date.game_date
    result = get_date_status(db_session, game_date)
    if result.failure:
        return result
    date_status = result.value
    update_date_status_record(date_status, games_for_date)
    result = create_game_status_records(db_session, games_for_date)
    if result.failure:
        return result
    return Result.Ok()


def update_date_status_record(date_status, games_for_date):
    date_status.scraped_daily_dash_bbref = 1
    date_status.game_count_bbref = games_for_date.game_count


def create_game_status_records(db_session, games_for_date):
    game_date = games_for_date.game_date
    for bbref_game_id in games_for_date.all_bbref_game_ids:
        result = get_date_status_from_bbref_game_id(db_session, bbref_game_id)
        if result.failure:
            return result
        date_status = result.value
        game_status = get_game_status(db_session, game_date, bbref_game_id, None)
        game_status.game_date = game_date
        game_status.bbref_game_id = bbref_game_id
        game_status.scrape_status_date_id = date_status.id
        game_status.season_id = date_status.season_id
    return Result.Ok()
