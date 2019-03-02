import os
from pathlib import Path
from unittest import TestCase

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.main.models.base import Base

APP_ROOT = Path.cwd()
DOTENV_PATH = APP_ROOT / '.env'


class BaseTestCase(TestCase):
    """Base Tests."""

    if DOTENV_PATH.is_file():
        load_dotenv(DOTENV_PATH)
    engine = create_engine(os.getenv('DATABASE_URL_TEST'))
    sessionmaker = sessionmaker(bind=engine)
    session = sessionmaker()

    def setUp(self):
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)

    def tearDown(self):
        self.session.close()
        Base.metadata.drop_all(self.engine)
