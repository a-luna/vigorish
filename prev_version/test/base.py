import json
import os
from pathlib import Path
from unittest import TestCase

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.main.models.base import Base


APP_FOLDER = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = APP_FOLDER / "config.json"


class BaseTestCase(TestCase):
    """Base Tests."""

    config = json.loads(CONFIG_PATH.read_text())
    engine = create_engine(config.get("DATABASE_URL_TEST"))
    sessionmaker = sessionmaker(bind=engine)
    session = sessionmaker()

    def setUp(self):
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)

    def tearDown(self):
        self.session.close()
        Base.metadata.drop_all(self.engine)
