from app.main.util.s3_helper import (
    get_all_scraped_bbref_game_ids_from_s3,
    get_bbref_boxscore_from_s3,
)
from app.main.models.status_game import GameScrapeStatus
from app.main.util.result import Result


def update_data_set_bbref_boxscores(session, season):
    result = get_all_scraped_bbref_game_ids_from_s3(season.year)
    if result.failure:
        return result
    scraped_bbref_gameids = result.value
    unscraped_bbref_gameids = GameScrapeStatus.get_all_unscraped_bbref_game_ids_for_season(
        session, season.id
    )
    new_bbref_game_ids = set(scraped_bbref_gameids) & set(unscraped_bbref_gameids)
    if not new_bbref_game_ids:
        return Result.Ok([])
    result = update_status_bbref_boxscore_list(session, new_bbref_game_ids)
    if result.failure:
        return result
    return Result.Ok()


def update_status_bbref_boxscore_list(session, new_bbref_game_ids):
    for bbref_game_id in new_bbref_game_ids:
        result = get_bbref_boxscore_from_s3(bbref_game_id)
        if result.failure:
            return result
        boxscore = result.value
        result = update_status_bbref_boxscore(session, boxscore)
        if result.failure:
            return result
    return Result.Ok()


def update_status_bbref_boxscore(session, boxscore):
    try:
        game_status = GameScrapeStatus.find_by_bbref_game_id(
            session, boxscore.bbref_game_id
        )
        if not game_status:
            error = f"scrape_status_game does not contain an entry for bbref_game_id: {boxscore.bbref_game_id}"
            return Result.Fail(error)
        setattr(game_status, "scraped_bbref_boxscore", 1)
        setattr(game_status, "pitch_app_count_bbref", boxscore.pitch_appearance_count)
        setattr(game_status, "total_pitch_count_bbref", boxscore.pitch_count)
        return Result.Ok()
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")
