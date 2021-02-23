import vigorish.database as db
from vigorish.enums import DataSet
from vigorish.util.result import Result


def update_status_bbref_boxscore_list(scraped_data, db_session, new_bbref_game_ids, apply_patch_list=True):
    for bbref_game_id in new_bbref_game_ids:
        boxscore = scraped_data.get_bbref_boxscore(bbref_game_id, apply_patch_list)
        if not boxscore:
            error = f"Failed to retrieve {DataSet.BBREF_BOXSCORES} (URL ID: {bbref_game_id})"
            return Result.Fail(error)
        result = update_status_bbref_boxscore(db_session, boxscore)
        if result.failure:
            return result
    return Result.Ok()


def update_status_bbref_boxscore(db_session, boxscore):
    result = update_game_status_records(db_session, boxscore)
    if result.failure:
        return result
    db_session.commit()
    return Result.Ok()


def update_game_status_records(db_session, boxscore):
    try:
        game_status = db.GameScrapeStatus.find_by_bbref_game_id(db_session, boxscore.bbref_game_id)
        if not game_status:
            error = f"scrape_status_game does not contain an entry for bbref_game_id: {boxscore.bbref_game_id}"
            return Result.Fail(error)
        game_status.scraped_bbref_boxscore = 1
        game_status.pitch_app_count_bbref = boxscore.pitch_appearance_count
        game_status.total_pitch_count_bbref = boxscore.pitch_count
        return Result.Ok()
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")
