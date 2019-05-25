from app.main.models.status_game import GameScrapeStatus
from app.main.util.s3_helper import get_all_brooks_pitch_logs_scraped, get_brooks_pitch_logs_for_game_from_s3
from app.main.util.result import Result


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
    for brooks_game_id in new_brooks_game_ids:
        result = get_brooks_pitch_logs_for_game_from_s3(brooks_game_id)
        if result.failure:
            return result
        pitch_logs_for_game = result.value
        result = update_status_brooks_pitch_logs_for_game(session, pitch_logs_for_game)
        if result.failure:
            return result
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
