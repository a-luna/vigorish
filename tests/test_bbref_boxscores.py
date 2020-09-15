from datetime import datetime

from vigorish.config.database import GameScrapeStatus
from vigorish.enums import DataSet
from vigorish.scrape.bbref_boxscores.models.boxscore import BBRefBoxscore
from vigorish.scrape.bbref_boxscores.parse_html import parse_bbref_boxscore
from vigorish.status.update_status_bbref_boxscores import update_status_bbref_boxscore
from vigorish.util.result import Result

BBREF_GAME_ID = "ATL201803290"
GAME_DATE = datetime(2018, 3, 29)


def get_bbref_boxscore_url(bbref_game_id):
    team_id = bbref_game_id[:3]
    return f"https://www.baseball-reference.com/boxes/{team_id}/{bbref_game_id}.shtml"


def parse_bbref_boxscore_from_html(scraped_data, bbref_game_id):
    url = get_bbref_boxscore_url(bbref_game_id)
    html_path = scraped_data.get_html(DataSet.BBREF_BOXSCORES, bbref_game_id)
    result = parse_bbref_boxscore(html_path.read_text(), url)
    assert result.success
    bbref_boxscore = result.value
    return bbref_boxscore


def test_parse_bbref_boxscore(scraped_data):
    bbref_boxscore = parse_bbref_boxscore_from_html(scraped_data, BBREF_GAME_ID)
    assert isinstance(bbref_boxscore, BBRefBoxscore)
    result = verify_bbref_boxscore_ATL201803290(bbref_boxscore)
    assert result.success


def test_persist_bbref_boxscore(scraped_data):
    bbref_boxscore_parsed = parse_bbref_boxscore_from_html(scraped_data, BBREF_GAME_ID)
    assert isinstance(bbref_boxscore_parsed, BBRefBoxscore)
    result = scraped_data.save_json(DataSet.BBREF_BOXSCORES, bbref_boxscore_parsed)
    assert result.success
    saved_file_dict = result.value
    json_filepath = saved_file_dict["local_filepath"]
    assert json_filepath.name == "ATL201803290.json"
    bbref_boxscore_decoded = scraped_data.get_bbref_boxscore(BBREF_GAME_ID)
    assert isinstance(bbref_boxscore_decoded, BBRefBoxscore)
    result = verify_bbref_boxscore_ATL201803290(bbref_boxscore_decoded)
    assert result.success
    json_filepath.unlink()
    assert not json_filepath.exists()


def test_update_database_bbref_boxscore(db_session, scraped_data):
    bbref_boxscore = parse_bbref_boxscore_from_html(scraped_data, BBREF_GAME_ID)
    assert isinstance(bbref_boxscore, BBRefBoxscore)
    game_status = GameScrapeStatus.find_by_bbref_game_id(db_session, BBREF_GAME_ID)
    assert game_status
    assert game_status.scraped_bbref_boxscore == 0
    assert game_status.pitch_app_count_bbref == 0
    assert game_status.total_pitch_count_bbref == 0
    result = update_status_bbref_boxscore(db_session, bbref_boxscore)
    assert result.success
    assert game_status.scraped_bbref_boxscore == 1
    assert game_status.pitch_app_count_bbref == 12
    assert game_status.total_pitch_count_bbref == 308
    # reset_game_scrape_status_after_parsed_boxscore(db_session, BBREF_GAME_ID)


def verify_bbref_boxscore_ATL201803290(bbref_boxscore):
    boxscore_url = get_bbref_boxscore_url(BBREF_GAME_ID)
    assert bbref_boxscore.bbref_game_id == BBREF_GAME_ID
    assert bbref_boxscore.boxscore_url == boxscore_url
    assert bbref_boxscore.away_team_data.team_id_br == "PHI"
    assert bbref_boxscore.away_team_data.total_runs_scored_by_team == 5
    assert bbref_boxscore.away_team_data.total_runs_scored_by_opponent == 8
    assert bbref_boxscore.away_team_data.total_wins_before_game == 0
    assert bbref_boxscore.away_team_data.total_losses_before_game == 1
    assert bbref_boxscore.away_team_data.total_hits_by_team == 6
    assert bbref_boxscore.away_team_data.total_hits_by_opponent == 9
    assert bbref_boxscore.away_team_data.total_errors_by_team == 1
    assert bbref_boxscore.away_team_data.total_errors_by_opponent == 0
    assert bbref_boxscore.home_team_data.team_id_br == "ATL"
    assert bbref_boxscore.home_team_data.total_runs_scored_by_team == 8
    assert bbref_boxscore.home_team_data.total_runs_scored_by_opponent == 5
    assert bbref_boxscore.home_team_data.total_wins_before_game == 1
    assert bbref_boxscore.home_team_data.total_losses_before_game == 0
    assert bbref_boxscore.home_team_data.total_hits_by_team == 9
    assert bbref_boxscore.home_team_data.total_hits_by_opponent == 6
    assert bbref_boxscore.home_team_data.total_errors_by_team == 0
    assert bbref_boxscore.home_team_data.total_errors_by_opponent == 1
    assert bbref_boxscore.game_meta_info.attendance == 40208
    assert bbref_boxscore.game_meta_info.park_name == "SunTrust Park"
    assert bbref_boxscore.game_meta_info.game_duration == "3:28"
    assert bbref_boxscore.game_meta_info.day_night == "Day Game"
    assert bbref_boxscore.game_meta_info.field_type == "On Grass"
    assert bbref_boxscore.game_meta_info.first_pitch_temperature == 74
    assert bbref_boxscore.game_meta_info.first_pitch_wind == "Wind 16mph from Left to Right"
    assert bbref_boxscore.game_meta_info.first_pitch_clouds == "Cloudy"
    assert bbref_boxscore.game_meta_info.first_pitch_precipitation == "No Precipitation"
    return Result.Ok()


# def reset_game_scrape_status_after_parsed_boxscore(db_session, bbref_game_id):
#     game_status = GameScrapeStatus.find_by_bbref_game_id(db_session, bbref_game_id)
#     setattr(game_status, "scraped_bbref_boxscore", 0)
#     setattr(game_status, "pitch_app_count_bbref", 0)
#     setattr(game_status, "total_pitch_count_bbref", 0)
#     db_session.commit()
