from vigorish.config.database import Season
from vigorish.enums import DataSet
from vigorish.scrape.brooks_pitchfx.parse_html import parse_pitchfx_log
from vigorish.status.update_status_bbref_boxscores import update_status_bbref_boxscore
from vigorish.status.update_status_bbref_games_for_date import (
    update_bbref_games_for_date_single_date as update_status_bbref_games_for_date,
)
from vigorish.status.update_status_brooks_games_for_date import (
    update_brooks_games_for_date_single_date as update_status_brooks_games_for_date,
)
from vigorish.status.update_status_brooks_pitch_logs import (
    update_status_brooks_pitch_logs_for_game,
)
from vigorish.status.update_status_brooks_pitchfx import update_pitch_appearance_status_records
from tests.test_bbref_games_for_date import parse_bbref_games_for_date_from_html
from tests.test_brooks_games_for_date import (
    parse_brooks_games_for_date_from_html,
    GAME_DATE as GAME_DATE_BB_DAILY,
)
from tests.test_bbref_boxscores import (
    parse_bbref_boxscore_from_html,
    GAME_DATE as GAME_DATE_BBREF_BOX,
)
from tests.test_brooks_pitch_logs import (
    parse_brooks_pitch_logs_for_game_from_html,
    BBREF_GAME_ID as GAME_ID_PLOG,
    GAME_DATE as GAME_DATE_PLOG,
)
from tests.test_brooks_pitchfx import (
    BBREF_GAME_ID as GAME_ID_PFX,
    GAME_DATE as GAME_DATE_PFX,
)
from tests.test_combine_scraped_data import COMBINED_DATA_GAME_DICT


def seed_database_with_test_data(db_session, scraped_data):
    seed_database_with_2018_test_data(db_session, scraped_data)
    seed_database_with_2019_test_data(db_session, scraped_data)


def seed_database_with_2018_test_data(db_session, scraped_data):
    season = Season.find_by_year(db_session, 2018)
    assert season
    # Seed data for test_brooks_games_for_date
    update_scraped_bbref_games_for_date(db_session, scraped_data, GAME_DATE_BB_DAILY, season)
    # Seed data for test_bbref_boxscores
    update_scraped_bbref_games_for_date(db_session, scraped_data, GAME_DATE_BBREF_BOX, season)
    update_scraped_brooks_games_for_date(db_session, scraped_data, GAME_DATE_BBREF_BOX, season)
    # Seed data for test_brooks_pitch_logs
    update_scraped_bbref_games_for_date(db_session, scraped_data, GAME_DATE_PLOG, season)
    update_scraped_brooks_games_for_date(db_session, scraped_data, GAME_DATE_PLOG, season)
    update_scraped_boxscore(db_session, scraped_data, GAME_ID_PLOG)
    # Seed data for test_brooks_pitchfx
    update_scraped_bbref_games_for_date(db_session, scraped_data, GAME_DATE_PFX, season)
    update_scraped_brooks_games_for_date(db_session, scraped_data, GAME_DATE_PFX, season)
    update_scraped_boxscore(db_session, scraped_data, GAME_ID_PFX)
    update_scraped_pitch_logs(db_session, scraped_data, GAME_DATE_PFX, GAME_ID_PFX)


def seed_database_with_2019_test_data(db_session, scraped_data):
    # seed data for test_combine_scraped_data
    season = Season.find_by_year(db_session, 2019)
    assert season
    for game_date, game_id_dict in COMBINED_DATA_GAME_DICT.items():
        bbref_game_id = game_id_dict["bbref_game_id"]
        bb_game_id = game_id_dict["bb_game_id"]
        update_scraped_bbref_games_for_date(db_session, scraped_data, game_date, season)
        update_scraped_brooks_games_for_date(db_session, scraped_data, game_date, season)
        update_scraped_boxscore(db_session, scraped_data, bbref_game_id)
        update_scraped_pitch_logs(db_session, scraped_data, game_date, bbref_game_id)
        update_scraped_pitchfx_logs(db_session, scraped_data, bb_game_id)


def update_scraped_bbref_games_for_date(db_session, scraped_data, game_date, season):
    games_for_date = parse_bbref_games_for_date_from_html(scraped_data, game_date)
    result = update_status_bbref_games_for_date(db_session, season, games_for_date)
    assert result.success


def update_scraped_brooks_games_for_date(db_session, scraped_data, game_date, season):
    games_for_date = parse_brooks_games_for_date_from_html(db_session, scraped_data, game_date)
    result = update_status_brooks_games_for_date(db_session, season, games_for_date)
    assert result.success


def update_scraped_boxscore(db_session, scraped_data, bbref_game_id):
    bbref_boxscore = parse_bbref_boxscore_from_html(scraped_data, bbref_game_id)
    result = update_status_bbref_boxscore(db_session, bbref_boxscore)
    assert result.success


def update_scraped_pitch_logs(db_session, scraped_data, game_date, bbref_game_id):
    pitch_logs_for_game = parse_brooks_pitch_logs_for_game_from_html(
        scraped_data, game_date, bbref_game_id
    )
    result = update_status_brooks_pitch_logs_for_game(db_session, pitch_logs_for_game)
    assert result.success


def update_scraped_pitchfx_logs(db_session, scraped_data, bb_game_id):
    pitch_logs = scraped_data.get_brooks_pitch_logs_for_game(bb_game_id)
    for pitch_log in pitch_logs.pitch_logs:
        if not pitch_log.parsed_all_info:
            continue
        html_path = scraped_data.get_html(DataSet.BROOKS_PITCHFX, pitch_log.pitch_app_id)
        result = parse_pitchfx_log(html_path.read_text(), pitch_log)
        assert result.success
        pitchfx_log = result.value
        result = update_pitch_appearance_status_records(db_session, pitchfx_log)
        assert result.success
