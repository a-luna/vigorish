"""App config definitions."""
import os
from os.path import join, dirname
from pathlib import Path

from app.main.data.scrape.bbref.scrape_bbref_games_for_date import (
    scrape_bbref_games_for_date
)
from app.main.data.scrape.brooks.scrape_brooks_games_for_date import (
    scrape_brooks_games_for_date
)
from app.main.util.s3_helper import (
    upload_brooks_games_for_date, get_brooks_games_for_date_from_s3,
    upload_bbref_games_for_date, get_bbref_games_for_date_from_s3
)


class DataSetConfig:
    DISPLAY_NAME = ''
    REQUIRES_INPUT = False
    REQUIRES_SELENIUM = False
    GET_INPUT_FUNCTION = None
    SCRAPE_FUNCTION = None
    PERSIST_FUNCTION = None

class BBrefGamesForDateConfig(DataSetConfig):
    DISPLAY_NAME = 'All games on date (bbref.com)'
    SCRAPE_FUNCTION = scrape_bbref_games_for_date
    PERSIST_FUNCTION = upload_bbref_games_for_date

class BBrefBoxscoreConfig(DataSetConfig):
    DISPLAY_NAME = 'Boxscore (bbref.com)'
    REQUIRES_INPUT = True
    REQUIRES_SELENIUM = True
    GET_INPUT_FUNCTION = get_bbref_games_for_date_from_s3

class BBrefPlayerConfig(DataSetConfig):
    DISPLAY_NAME = 'Player bio (bbref.com)'
    REQUIRES_INPUT = True
    REQUIRES_SELENIUM = True

class BrooksGamesForDateConfig(DataSetConfig):
    DISPLAY_NAME = 'All games on date (brooksbaseball.com)'
    SCRAPE_FUNCTION = scrape_brooks_games_for_date
    PERSIST_FUNCTION = upload_brooks_games_for_date

class BrooksPitchLogConfig(DataSetConfig):
    DISPLAY_NAME = 'Pitch log (brooksbaseball.com)'
    REQUIRES_INPUT = True
    GET_INPUT_FUNCTION = get_brooks_games_for_date_from_s3

class BrooksPitchFxConfig(DataSetConfig):
    DISPLAY_NAME = 'PitchFX (brooksbaseball.com)'
    REQUIRES_INPUT = True

scrape_config_by_data_set = dict(
    bbref_games_for_date=BBrefGamesForDateConfig,
    bbref_boxscore=BBrefBoxscoreConfig,
    bbref_player=BBrefPlayerConfig,
    brooks_games_for_date=BrooksGamesForDateConfig,
    brooks_pitch_log=BrooksPitchLogConfig,
    brooks_pitchfx=BrooksPitchFxConfig
)

