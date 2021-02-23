import vigorish.database as db
from vigorish.util.result import Result


def update_status_brooks_pitch_logs_for_game_list(scraped_data, db_session, new_brooks_game_ids):
    plog_missing_json, plog_missing_db_records = [], []
    for bb_game_id in new_brooks_game_ids:
        pitch_logs_for_game = scraped_data.get_brooks_pitch_logs_for_game(bb_game_id)
        if not pitch_logs_for_game:
            plog_missing_json.append(bb_game_id)
            continue
        result = update_game_status_records(db_session, pitch_logs_for_game)
        if result.failure:
            plog_missing_db_records.append(bb_game_id)
            continue
        result = create_pitch_appearance_status_records(db_session, pitch_logs_for_game)
        if result.failure:
            return result
        db_session.commit()
    return (
        Result.Ok()
        if not (plog_missing_json or plog_missing_db_records)
        else Result.Fail(_get_error_message(plog_missing_json, plog_missing_db_records))
    )


def _get_error_message(plog_missing_json, plog_missing_db_records):
    errors = []
    if plog_missing_json:
        errors.append(f"Failed to retrieve JSON files for pitch logs: {','.join(plog_missing_json)}")
    if plog_missing_db_records:
        errors.append(f"bb_game_id not found in scrape_status_game table: {','.join(plog_missing_db_records)}")
    return "\n".join(errors)


def update_status_brooks_pitch_logs_for_game(db_session, pitch_logs_for_game):
    result = update_game_status_records(db_session, pitch_logs_for_game)
    if result.failure:
        return result
    result = create_pitch_appearance_status_records(db_session, pitch_logs_for_game)
    if result.failure:
        return result
    db_session.commit()
    return Result.Ok()


def update_game_status_records(db_session, pitch_logs_for_game):
    try:
        bb_game_id = pitch_logs_for_game.bb_game_id
        game_status = db.GameScrapeStatus.find_by_bb_game_id(db_session, bb_game_id)
        if not game_status:
            error = f"scrape_status_game does not contain an entry for bb_game_id: {bb_game_id}"
            return Result.Fail(error)
        game_status.scraped_brooks_pitch_logs = 1
        return Result.Ok()
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")


def create_pitch_appearance_status_records(db_session, pitch_logs_for_game):
    season = None
    bbref_game_id = pitch_logs_for_game.bbref_game_id
    all_pitch_app_ids = db.PitchAppScrapeStatus.get_all_pitch_app_ids_for_game(db_session, bbref_game_id)
    scraped_pitch_app_ids = [plog.pitch_app_id for plog in pitch_logs_for_game.pitch_logs]
    update_pitch_app_ids = set(scraped_pitch_app_ids).difference(all_pitch_app_ids)
    for pitch_log in pitch_logs_for_game.pitch_logs:
        if pitch_log.pitch_app_id not in update_pitch_app_ids:
            continue
        if not season:
            season = db.Season.find_by_year(db_session, pitch_log.game_date.year)
        try:
            date_status = db.DateScrapeStatus.find_by_date(db_session, pitch_log.game_date)
            game_status = db.GameScrapeStatus.find_by_bb_game_id(db_session, pitch_log.bb_game_id)
            player_id = db.PlayerId.find_by_mlb_id(db_session, pitch_log.pitcher_id_mlb)
            pitch_app_status = db.PitchAppScrapeStatus()
            pitch_app_status.pitcher_id_mlb = pitch_log.pitcher_id_mlb
            pitch_app_status.pitch_app_id = pitch_log.pitch_app_id
            pitch_app_status.bbref_game_id = pitch_log.bbref_game_id
            pitch_app_status.bb_game_id = pitch_log.bb_game_id
            pitch_app_status.pitch_count_pitch_log = pitch_log.total_pitch_count
            pitch_app_status.scrape_status_game_id = game_status.id
            pitch_app_status.scrape_status_date_id = date_status.id
            pitch_app_status.pitcher_id = player_id.db_player_id
            pitch_app_status.season_id = season.id
            if not pitch_log.parsed_all_info:
                pitch_app_status.scraped_pitchfx = 1
                pitch_app_status.no_pitchfx_data = 1
            db_session.add(pitch_app_status)
        except Exception as e:
            return Result.Fail(f"Error: {repr(e)}")
    return Result.Ok()
