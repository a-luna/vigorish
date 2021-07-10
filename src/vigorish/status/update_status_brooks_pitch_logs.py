from vigorish.status.util import (
    get_game_status,
    get_pitch_app_status,
    get_player_id,
)
from vigorish.util.result import Result


def update_status_brooks_pitch_logs_for_game_list(scraped_data, db_session, new_brooks_game_ids):
    missing_json = []
    for bb_game_id in new_brooks_game_ids:
        pitch_logs_for_game = scraped_data.get_brooks_pitch_logs_for_game(bb_game_id)
        if not pitch_logs_for_game:
            missing_json.append(bb_game_id)
            continue
        update_status_brooks_pitch_logs_for_game(db_session, pitch_logs_for_game)
    return (
        Result.Ok()
        if not missing_json
        else Result.Fail(f"Failed to retrieve Brooks Pitch Logs for Game JSON files: {','.join(missing_json)}")
    )


def update_status_brooks_pitch_logs_for_game(db_session, pitch_logs_for_game):
    game_date = pitch_logs_for_game.game_date
    bbref_game_id = pitch_logs_for_game.bbref_game_id
    bb_game_id = pitch_logs_for_game.bb_game_id
    game_status = get_game_status(db_session, game_date, bbref_game_id, bb_game_id)
    update_game_status_record(game_status)
    update_pitch_app_status_records(db_session, pitch_logs_for_game, game_status)


def update_game_status_record(game_status):
    game_status.scraped_brooks_pitch_logs = 1


def update_pitch_app_status_records(db_session, pitch_logs_for_game, game_status):
    for pitch_log in pitch_logs_for_game.pitch_logs:
        bbref_game_id = pitch_log.bbref_game_id
        bb_game_id = pitch_log.bb_game_id
        pitch_app_id = pitch_log.pitch_app_id
        player_id = get_player_id(db_session, mlb_id=pitch_log.pitcher_id_mlb)
        pitch_app_status = get_pitch_app_status(
            db_session, bbref_game_id, bb_game_id, game_status, player_id, pitch_app_id
        )
        update_pitch_app_status_record(pitch_app_status, pitch_log)


def update_pitch_app_status_record(pitch_app_status, pitch_log):
    pitch_app_status.pitch_count_pitch_log = pitch_log.total_pitch_count
    if not pitch_log.parsed_all_info:
        pitch_app_status.scraped_pitchfx = 1
        pitch_app_status.no_pitchfx_data = 1
