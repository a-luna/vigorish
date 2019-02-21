"""App config definitions."""
import os
from os.path import join, dirname
from pathlib import Path


APP_MAIN = Path.cwd()
APP_ROOT = str(APP_MAIN.parent.parent)
APP_MAIN_ABS = str(APP_MAIN)
sqlite_url = 'sqlite:///' / APP_MAIN / 'vig_test.db'

class Config:
    DEBUG = False
    TESTING = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False

class DevelopmentConfig(Config):
    ENV = 'dev'
    DEBUG = True
    TESTING = False
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL')


class TestingConfig(Config):
    ENV = 'test'
    DEBUG = True
    TESTING = True
    SQLALCHEMY_DATABASE_URI = sqlite_url
    PRESERVE_CONTEXT_ON_EXCEPTION = False


class ProductionConfig(Config):
    ENV = 'prod'
    DEBUG = False
    TESTING = False
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL')


config_by_name = dict(
    dev=DevelopmentConfig,
    test=TestingConfig,
    prod=ProductionConfig
)

key = Config.SECRET_KEY
