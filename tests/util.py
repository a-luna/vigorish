from tests.test_bbref_boxscores import GAME_DATE as GAME_DATE_BBREF_BOX
from tests.test_bbref_boxscores import parse_bbref_boxscore_from_html
from tests.test_bbref_games_for_date import parse_bbref_games_for_date_from_html
from tests.test_brooks_games_for_date import GAME_DATE as GAME_DATE_BB_DAILY
from tests.test_brooks_games_for_date import parse_brooks_games_for_date_from_html
from tests.test_brooks_pitch_logs import BBREF_GAME_ID as GAME_ID_PLOG
from tests.test_brooks_pitch_logs import GAME_DATE as GAME_DATE_PLOG
from tests.test_brooks_pitch_logs import parse_brooks_pitch_logs_for_game_from_html
from tests.test_brooks_pitchfx import BBREF_GAME_ID as GAME_ID_PFX
from tests.test_brooks_pitchfx import GAME_DATE as GAME_DATE_PFX
from tests.test_combine_scraped_data import COMBINED_DATA_GAME_DICT
from vigorish.config.database import Season
from vigorish.enums import DataSet
from vigorish.models.status_pitch_appearance import PitchAppScrapeStatus
from vigorish.scrape.brooks_pitchfx.parse_html import parse_pitchfx_log
from vigorish.status.update_status_bbref_boxscores import update_status_bbref_boxscore
from vigorish.status.update_status_bbref_games_for_date import (
    update_bbref_games_for_date_single_date as update_status_bbref_games_for_date,
)
from vigorish.status.update_status_brooks_games_for_date import (
    update_brooks_games_for_date_single_date as update_status_brooks_games_for_date,
)
from vigorish.status.update_status_brooks_pitch_logs import update_status_brooks_pitch_logs_for_game
from vigorish.status.update_status_brooks_pitchfx import update_pitch_appearance_status_records
from vigorish.util.result import Result


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


def revert_pitch_logs_to_state_before_combined_data(db_session, scraped_data, bbref_game_id):
    result = scraped_data.get_all_pitchfx_logs_for_game(bbref_game_id)
    assert result.success
    pfx_logs = result.value
    for pfx_log in pfx_logs:
        pitch_app_id = pfx_log.pitch_app_id
        pitch_app_status = PitchAppScrapeStatus.find_by_pitch_app_id(db_session, pitch_app_id)
        assert pitch_app_status
        if not pitch_app_status.combined_pitchfx_bbref_data:
            continue
        pitch_app_status.combined_pitchfx_bbref_data = 0
        pitch_app_status.pitch_count_bbref = 0
        pitch_app_status.pitch_count_pitchfx_audited = 0
        pitch_app_status.duplicate_guid_removed_count = 0
        pitch_app_status.missing_pitchfx_count = 0
        pitch_app_status.batters_faced_bbref = 0
        pitch_app_status.total_at_bats_pitchfx_complete = 0
        pitch_app_status.total_at_bats_missing_pitchfx = 0
    return Result.Ok()
