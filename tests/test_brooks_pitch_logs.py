from datetime import datetime

from vigorish.enums import DataSet
from vigorish.scrape.brooks_pitch_logs.models.pitch_logs_for_game import BrooksPitchLogsForGame
from vigorish.scrape.brooks_pitch_logs.models.pitch_log import BrooksPitchLog
from vigorish.scrape.brooks_pitch_logs.parse_html import parse_pitch_log
from vigorish.util.result import Result

DATA_SET = DataSet.BROOKS_PITCH_LOGS
GAME_DATE = datetime(2018, 6, 17)
BB_GAME_ID = "gid_2018_06_17_wasmlb_tormlb_1"


def parse_brooks_pitch_logs_for_game_from_html(scraped_data):
    result = scraped_data.get_brooks_games_for_date(GAME_DATE)
    assert result.success
    brooks_games_for_date = result.value
    game_info = [game for game in brooks_games_for_date.games if game.bb_game_id == BB_GAME_ID][0]
    pitch_logs_for_game = BrooksPitchLogsForGame()
    pitch_logs_for_game.bb_game_id = game_info.bb_game_id
    pitch_logs_for_game.bbref_game_id = game_info.bbref_game_id
    pitch_logs_for_game.pitch_log_count = game_info.pitcher_appearance_count
    scraped_pitch_logs = []
    for pitcher_id, url in game_info.pitcher_appearance_dict.items():
        pitch_app_id = f"{game_info.bbref_game_id}_{pitcher_id}"
        html_path = scraped_data.get_html(DATA_SET, pitch_app_id)
        result = parse_pitch_log(html_path.read_text(), game_info, pitcher_id, url)
        assert result.success
        pitch_log = result.value
        assert isinstance(pitch_log, BrooksPitchLog)
        scraped_pitch_logs.append(pitch_log)
    pitch_logs_for_game.pitch_logs = scraped_pitch_logs
    return pitch_logs_for_game


def test_parse_brooks_pitch_logs_for_game(scraped_data):
    pitch_logs_for_game = parse_brooks_pitch_logs_for_game_from_html(scraped_data)
    assert isinstance(pitch_logs_for_game, BrooksPitchLogsForGame)
    result = verify_brooks_pitch_logs_for_game_TOR201806170(pitch_logs_for_game)
    assert result.success


def test_persist_brooks_pitch_logs_for_game(scraped_data):
    pitch_logs_for_game_parsed = parse_brooks_pitch_logs_for_game_from_html(scraped_data)
    assert isinstance(pitch_logs_for_game_parsed, BrooksPitchLogsForGame)
    result = verify_brooks_pitch_logs_for_game_TOR201806170(pitch_logs_for_game_parsed)
    result = scraped_data.save_json(DATA_SET, pitch_logs_for_game_parsed)
    assert result.success
    json_filepath = result.value
    assert json_filepath.name == "gid_2018_06_17_wasmlb_tormlb_1.json"
    result = scraped_data.get_brooks_pitch_logs_for_game(BB_GAME_ID)
    assert result.success
    pitch_logs_for_game_decoded = result.value
    assert isinstance(pitch_logs_for_game_decoded, BrooksPitchLogsForGame)
    result = verify_brooks_pitch_logs_for_game_TOR201806170(pitch_logs_for_game_decoded)
    assert result.success
    json_filepath.unlink()
    assert not json_filepath.exists()


def verify_brooks_pitch_logs_for_game_TOR201806170(pitch_logs_for_game):
    pitch_logs = pitch_logs_for_game.pitch_logs
    assert len(pitch_logs) == 11
    assert pitch_logs[0].pitcher_name == "Tanner Roark"
    assert pitch_logs[0].pitcher_id_mlb == 543699
    assert pitch_logs[0].total_pitch_count == 96
    assert pitch_logs[0].pitch_count_by_inning == {"1": 23, "2": 16, "3": 37, "4": 20}
    assert pitch_logs[9].pitcher_name == "Tyler Clippard"
    assert pitch_logs[9].pitcher_id_mlb == 461325
    assert pitch_logs[9].total_pitch_count == 25
    assert pitch_logs[9].pitch_count_by_inning == {"8": 25}
    return Result.Ok()