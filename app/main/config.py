"""App config definitions."""
import os
from os.path import join, dirname
from pathlib import Path

from app.main.scrape.bbref.scrape_bbref_boxscores_for_date import (
    scrape_bbref_boxscores_for_date
)
from app.main.scrape.bbref.scrape_bbref_games_for_date import (
    scrape_bbref_games_for_date
)
from app.main.scrape.brooks.scrape_brooks_games_for_date import (
    scrape_brooks_games_for_date
)
from app.main.scrape.brooks.scrape_brooks_pitch_logs_for_date import (
    scrape_brooks_pitch_logs_for_date
)
from app.main.status.update_status import (
    update_status_bbref_games_for_date,
    update_status_brooks_games_for_date,
    update_status_bbref_boxscores_list,
    update_status_brooks_pitch_logs_for_game_list
)
from app.main.util.result import Result
from app.main.util.s3_helper import (
    upload_brooks_games_for_date, get_brooks_games_for_date_from_s3,
    upload_bbref_games_for_date, get_bbref_games_for_date_from_s3,
    upload_bbref_boxscore, get_bbref_boxscore_from_s3,
    upload_brooks_pitch_logs_for_game, get_brooks_pitch_logs_for_game_from_s3
)
from app.main.util.scrape_functions import get_chromedriver


class ScrapeConfig:
    display_name = ''
    requires_input = False
    requires_selenium = False
    produces_list = False
    get_input_function = None
    scrape_function = None
    persist_function = None
    populate_function = None
    update_status_function = None
    driver = None

class BBRefGamesForDateScrapeConfig(ScrapeConfig):
    display_name = 'Games for date (bbref.com)'
    scrape_function = scrape_bbref_games_for_date
    persist_function = upload_bbref_games_for_date
    update_status_function = update_status_bbref_games_for_date

class BBRefBoxscoreScrapeConfig(ScrapeConfig):
    display_name = 'Boxscores for date (bbref.com)'
    requires_input = True
    requires_selenium = True
    produces_list = True
    get_input_function = get_bbref_games_for_date_from_s3
    scrape_function = scrape_bbref_boxscores_for_date
    persist_function = upload_bbref_boxscore
    update_status_function = update_status_bbref_boxscores_list

class BBRefPlayerScrapeConfig(ScrapeConfig):
    display_name = 'Player bio (bbref.com)'
    requires_input = True
    requires_selenium = True

class BrooksGamesForDateScrapeConfig(ScrapeConfig):
    display_name = 'Games for date (brooksbaseball.com)'
    requires_selenium = True
    scrape_function = scrape_brooks_games_for_date
    persist_function = upload_brooks_games_for_date
    update_status_function = update_status_brooks_games_for_date

class BrooksPitchLogScrapeConfig(ScrapeConfig):
    display_name = 'Pitch logs for date (brooksbaseball.com)'
    requires_input = True
    produces_list = True
    get_input_function = get_brooks_games_for_date_from_s3
    scrape_function = scrape_brooks_pitch_logs_for_date
    persist_function = upload_brooks_pitch_logs_for_game
    update_status_function = update_status_brooks_pitch_logs_for_game_list

class BrooksPitchFxScrapeConfig(ScrapeConfig):
    display_name = 'PitchFX for pitching appearance (brooksbaseball.com)'
    requires_input = True

SCRAPE_CONFIG_DICT = dict(
    bbref_games_for_date=BBRefGamesForDateScrapeConfig,
    bbref_boxscore=BBRefBoxscoreScrapeConfig,
    bbref_player=BBRefPlayerScrapeConfig,
    brooks_games_for_date=BrooksGamesForDateScrapeConfig,
    brooks_pitch_log=BrooksPitchLogScrapeConfig,
    brooks_pitchfx=BrooksPitchFxScrapeConfig
)

def get_config(data_set):
    if not data_set in SCRAPE_CONFIG_DICT:
        error = f'Invalid value for data-set: "{data_set}"'
        return Result.Fail(error)
    config = SCRAPE_CONFIG_DICT[data_set]
    if config.requires_selenium:
        result = get_chromedriver()
        if result.failure:
            return result
        config.driver = result.value
    return Result.Ok(config)