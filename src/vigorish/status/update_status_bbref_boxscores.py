import vigorish.database as db
from vigorish.status.util import (
    get_game_status,
    get_pitch_app_status,
)
from vigorish.tasks.scrape_mlb_player_info import ScrapeMlbPlayerInfoTask
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


def update_pitch_app_status_records(db_session, boxscore, game_status):
    all_pitch_stats = boxscore.away_team_data.pitching_stats + boxscore.home_team_data.pitching_stats
    for pitch_stats in all_pitch_stats:
        bbref_game_id = boxscore.bbref_game_id
        bb_game_id = boxscore.bb_game_id
        player_id = db.PlayerId.find_by_bbref_id(db_session, pitch_stats.player_id_br)
        if not player_id:
            bbref_id = pitch_stats.player_id_br
            name_with_team = [k for k, v in boxscore.player_name_dict.items() if v == bbref_id][0]
            name = name_with_team.split(",")[0]
            scrape_player_task = ScrapeMlbPlayerInfoTask(get_mock_app(db_session))
            result = scrape_player_task.execute(name, bbref_id, game_status.date)
            if result.failure:
                return result
            player = result.value
            player_id = player.id_map
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


def get_mock_app(db_session):
    class MockApp:
        def __init__(self, db_session):
            self.dotenv = ""
            self.config = ""
            self.db_engine = ""
            self.db_session = db_session
            self.scraped_data = ""

    return MockApp(db_session)
