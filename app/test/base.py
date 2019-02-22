from pathlib import Path
from unittest import TestCase

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.main.models.base import Base

TEST_FOLDER = Path.cwd() / 'app' / 'test'
SQLITE_FILEPATH = TEST_FOLDER / 'vig_test.db'
SQLITE_URI = f'sqlite:///{SQLITE_FILEPATH}'

class BaseTestCase(TestCase):
    """Base Tests."""

    engine = create_engine(str(SQLITE_URI))
    sessionmaker = sessionmaker(bind=engine)
    session = sessionmaker()

    def setUp(self):
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)

    def tearDown(self):
        self.session.close()
        Base.metadata.drop_all(self.engine)
        SQLITE_FILEPATH.unlink()
