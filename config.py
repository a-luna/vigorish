"""App config definitions."""
import os
from os.path import join, dirname
from pathlib import Path

from dotenv import load_dotenv

APP_ROOT = Path.cwd()
DOTENV_PATH = APP_ROOT / '.env'
APP_MAIN = APP_ROOT / 'app' / 'main'
sqlite_url = 'sqlite:///' / APP_MAIN / 'vig_test.db'

class Config:
    DEBUG = False
    TESTING = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL')

class DevelopmentConfig(Config):
    DEBUG = True
    TESTING = False


class TestingConfig(Config):
    DEBUG = True
    TESTING = True
    SQLALCHEMY_DATABASE_URI = str(sqlite_url)
    PRESERVE_CONTEXT_ON_EXCEPTION = False


class ProductionConfig(Config):
    DEBUG = False
    TESTING = False


config_by_name = dict(
    dev=DevelopmentConfig,
    test=TestingConfig,
    prod=ProductionConfig
)

