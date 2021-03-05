from tests.test_with_single_game.conftest import BBREF_GAME_ID
from vigorish.database import GameScrapeStatus


def test_status_game_status_report(vig_app):
    status_game = GameScrapeStatus.find_by_bbref_game_id(vig_app.db_session, BBREF_GAME_ID)
    assert status_game
    assert status_game.bbref_game_id == BBREF_GAME_ID
    report = status_game.status_report()
    assert report == [
        "BBRef Game ID................................: TOR201906170",
        "brooksbaseball.net Game ID...................: gid_2019_06_17_anamlb_tormlb_1",
        "Game Date-Time...............................: 06/17/2019 07:07 PM EDT-0400",
        "Scraped BBRef Boxscore.......................: YES",
        "Scraped Brooks Pitch Logs....................: YES",
        "PitchFx Logs Scraped.........................: YES 12/12",
        "Combined BBRef/PitchFX Data..................: YES",
        "PitchFX Data Errors (Valid AB/Invalid AB)....: NO 0/0",
        "Pitch App Count (BBRef/Brooks)...............: 12/12",
        "Pitch App Count (PFx/data/no data)...........: 12/12/0",
        "Pitch Count (BBRef/Brooks/PFx)...............: 298/299/299",
        "Pitch Count Audited (BBRef/PFx/Removed)......: 298/298/1",
    ]


def test_status_game_as_dict(vig_app):
    status_game = GameScrapeStatus.find_by_bbref_game_id(vig_app.db_session, BBREF_GAME_ID)
    assert status_game
    assert status_game.bbref_game_id == BBREF_GAME_ID
    game_dict = status_game.as_dict()
    assert game_dict
    assert game_dict == {
        "bb_game_id": "gid_2019_06_17_anamlb_tormlb_1",
        "bbref_game_id": "TOR201906170",
        "combined_data_fail": 0,
        "combined_data_for_all_pitchfx_logs": True,
        "combined_data_success": 1,
        "game_date_time_str": "06/17/2019 07:07 PM EDT-0400",
        "pitch_app_count_bbref": 12,
        "pitch_app_count_brooks": 12,
        "pitch_app_count_pitchfx": 12,
        "pitchfx_error_for_any_pitchfx_logs": False,
        "pitchfx_is_valid_for_all_pitchfx_logs": True,
        "scraped_all_pitchfx_logs": True,
        "scraped_bbref_boxscore": 1,
        "scraped_brooks_pitch_logs": 1,
        "total_at_bats_removed_pitchfx": 1,
        "total_at_bats_invalid_pitchfx": 0,
        "total_at_bats_missing_pitchfx": 0,
        "total_at_bats_pitchfx_complete": 82,
        "total_at_bats_pitchfx_error": 0,
        "total_batters_faced_bbref": 82,
        "total_batters_faced_pitchfx": 82,
        "total_removed_pitchfx_count": 1,
        "total_missing_pitchfx_count": 0,
        "total_pitch_apps_combined_data": 12,
        "total_pitch_apps_invalid_pitchfx": 0,
        "total_pitch_apps_no_pitchfx_data": 0,
        "total_pitch_apps_pitchfx_error": 0,
        "total_pitch_apps_pitchfx_is_valid": 12,
        "total_pitch_apps_scraped_pitchfx": 12,
        "total_pitch_apps_with_pitchfx_data": 12,
        "total_pitch_count_bbref": 298,
        "total_pitch_count_bbref_audited": 298,
        "total_pitch_count_pitch_logs": 299,
        "total_pitch_count_pitchfx": 299,
        "total_pitch_count_pitchfx_audited": 298,
    }
