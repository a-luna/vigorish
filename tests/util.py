from tests.test_bbref_games_for_date import (
    reset_date_scrape_status_after_parsed_bbref_games_for_date,
    GAME_DATE,
)
from tests.test_brooks_games_for_date import (
    reset_date_scrape_status_after_parsed_brooks_games_for_date,
    reset_game_scrape_status_after_parsed_brooks_games_for_date,
    GAME_DATE as GAME_DATE_BB,
)
from tests.test_bbref_boxscores import (
    reset_game_scrape_status_after_parsed_boxscore,
    BBREF_GAME_ID,
)
from tests.test_brooks_pitch_logs import (
    reset_game_scrape_status_after_parsed_pitch_logs,
    reset_pitch_app_scrape_status_after_parsed_pitch_logs,
    BBREF_GAME_ID as BBREF_GAME_ID_PLOG,
    BB_GAME_ID,
)
from tests.test_brooks_pitchfx import (
    reset_pitch_app_scrape_status_after_parsed_pitchfx,
    PITCH_APP_ID,
)
from tests.test_combine_scraped_data import (
    reset_pitch_app_scrape_status_after_combined_data,
    GAME_ID_NO_ERRORS,
)


def reset_database_before_session_start(db_session):
    reset_date_scrape_status_after_parsed_bbref_games_for_date(db_session, GAME_DATE)
    reset_date_scrape_status_after_parsed_brooks_games_for_date(db_session, GAME_DATE_BB)
    reset_game_scrape_status_after_parsed_brooks_games_for_date(db_session, GAME_DATE_BB)
    reset_game_scrape_status_after_parsed_boxscore(db_session, BBREF_GAME_ID)
    reset_game_scrape_status_after_parsed_pitch_logs(db_session, BB_GAME_ID)
    reset_pitch_app_scrape_status_after_parsed_pitch_logs(db_session, BBREF_GAME_ID_PLOG)
    reset_pitch_app_scrape_status_after_parsed_pitchfx(db_session, PITCH_APP_ID)
    reset_pitch_app_scrape_status_after_combined_data(db_session, GAME_ID_NO_ERRORS)
