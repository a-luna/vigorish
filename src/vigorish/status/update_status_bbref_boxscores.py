from vigorish.status.util import (
    get_game_status,
    get_pitch_app_status,
    get_player_id,
)
from vigorish.util.result import Result


def update_status_bbref_boxscore_list(scraped_data, db_session, new_bbref_game_ids, apply_patch_list=True):
    missing_json = []
    for bbref_game_id in new_bbref_game_ids:
        boxscore = scraped_data.get_bbref_boxscore(bbref_game_id, apply_patch_list)
        if not boxscore:
            missing_json.append(bbref_game_id)
            continue
        update_status_bbref_boxscore(db_session, boxscore)
    return (
        Result.Ok()
        if not missing_json
        else Result.Fail(f"Failed to retrieve BBRef Boxscore JSON files: {','.join(missing_json)}")
    )


def update_status_bbref_boxscore(db_session, boxscore):
    game_date = boxscore.game_date
    bbref_game_id = boxscore.bbref_game_id
    bb_game_id = boxscore.bb_game_id
    game_status = get_game_status(db_session, game_date, bbref_game_id, bb_game_id)
    update_game_status_record(game_status, boxscore)
    update_pitch_app_status_records(db_session, boxscore, game_status)
    return Result.Ok()


def update_game_status_record(game_status, boxscore):
    game_status.scraped_bbref_boxscore = 1
    game_status.pitch_app_count_bbref = boxscore.pitch_appearance_count
    game_status.total_pitch_count_bbref = boxscore.pitch_count
    game_status.away_team_id_br = boxscore.away_team_data.team_id_br
    game_status.home_team_id_br = boxscore.home_team_data.team_id_br
    game_status.away_team_runs_scored = boxscore.away_team_data.total_runs_scored_by_team
    game_status.home_team_runs_scored = boxscore.home_team_data.total_runs_scored_by_team


def update_pitch_app_status_records(db_session, boxscore, game_status):
    all_pitch_stats = boxscore.away_team_data.pitching_stats + boxscore.home_team_data.pitching_stats
    for pitch_stats in all_pitch_stats:
        bbref_game_id = boxscore.bbref_game_id
        bb_game_id = boxscore.bb_game_id
        player_id = get_player_id(db_session, bbref_id=pitch_stats.player_id_br, boxscore=boxscore)
        pitch_app_id = f"{boxscore.bbref_game_id}_{player_id.mlb_id}"
        pitch_app_status = get_pitch_app_status(
            db_session, bbref_game_id, bb_game_id, game_status, player_id, pitch_app_id
        )
        update_pitch_app_status_record(pitch_app_status, pitch_stats)


def update_pitch_app_status_record(pitch_app_status, pitch_stats):
    pitch_app_status.pitch_count_bbref = pitch_stats.pitch_count
    pitch_app_status.batters_faced_bbref = pitch_stats.batters_faced
    if pitch_stats.pitch_count == 0 and pitch_stats.batters_faced == 0:
        pitch_app_status.scraped_pitchfx = 1
        pitch_app_status.no_pitchfx_data = 1
