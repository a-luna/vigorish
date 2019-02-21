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

    config = {}
    def setUp(self):
        if DOTENV_PATH.is_file():
            load_dotenv(DOTENV_PATH)
        engine = create_engine(os.getenv('DATABASE_URL'))
        session = sessionmaker(bind=engine)
        self.config = {
            'engine': engine,
            'session': session()
        }
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)

    def tearDown(self):
        self.config['session'].close()
        Base.metadata.drop_all(self.config['engine'])

