from vigorish.status.util import (
    get_game_status,
    get_pitch_app_status,
    get_player_id,
)
from vigorish.util.result import Result


def update_status_brooks_pitchfx_log_list(scraped_data, db_session, new_pitch_app_ids, apply_patch_list=True):
    missing_json = []
    for pitch_app_id in new_pitch_app_ids:
        pitchfx_log = scraped_data.get_brooks_pitchfx_log(pitch_app_id, apply_patch_list)
        if not pitchfx_log:
            missing_json.append(pitch_app_id)
            continue
        update_status_brooks_pitchfx_log(db_session, pitchfx_log)
    return (
        Result.Ok()
        if not missing_json
        else Result.Fail(f"Failed to retrieve Brooks PitchFx Logs JSON files: {','.join(missing_json)}")
    )


def update_status_brooks_pitchfx_log(db_session, pitchfx_log):
    bbref_game_id = pitchfx_log.bbref_game_id
    bb_game_id = pitchfx_log.bb_game_id
    game_date = pitchfx_log.game_date
    pitch_app_id = pitchfx_log.pitch_app_id
    player_id = get_player_id(db_session, mlb_id=pitchfx_log.pitcher_id_mlb)
    game_status = get_game_status(db_session, game_date, bbref_game_id, bb_game_id)
    pitch_app_status = get_pitch_app_status(db_session, bbref_game_id, bb_game_id, game_status, player_id, pitch_app_id)
    update_pitch_app_status_record(pitch_app_status, pitchfx_log)
    return Result.Ok()


def update_pitch_app_status_record(pitch_app_status, pitchfx_log):
    pitch_app_status.scraped_pitchfx = 1
    pitch_app_status.pitch_count_pitchfx = pitchfx_log.total_pitch_count
