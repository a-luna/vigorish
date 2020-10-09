from vigorish.data.all_game_data import AllGameData

from tests.test_combine_scraped_data import GAME_ID_NO_ERRORS


def test_all_game_data(db_session, scraped_data):
    all_game_data = AllGameData(db_session, scraped_data, GAME_ID_NO_ERRORS)
    pitch_app_player_ids = all_game_data.all_player_ids_with_pitch_stats
    pitch_app_player_id = pitch_app_player_ids[0]
    result = all_game_data.view_at_bats_for_pitcher(pitch_app_player_id)
    assert result.success
    pitch_app_at_bat_viewer = result.value
    assert pitch_app_at_bat_viewer

    bat_stats_player_ids = all_game_data.all_player_ids_with_bat_stats
    bat_stats_player_id = bat_stats_player_ids[0]
    result = all_game_data.view_at_bats_for_batter(bat_stats_player_id)
    assert result.success
    bat_stats_at_bat_viewer = result.value
    assert bat_stats_at_bat_viewer
