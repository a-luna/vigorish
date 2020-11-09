from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from vigorish.config.config_file import ConfigFile
from vigorish.config.database import get_db_url
from vigorish.config.dotenv_file import DotEnvFile
from vigorish.data.scraped_data import ScrapedData


def create_app(db_url=None):
    dotenv = DotEnvFile()
    config = ConfigFile()
    if not db_url:
        db_url = get_db_url()
    db_engine = create_engine(db_url)
    session_maker = sessionmaker(bind=db_engine)
    db_session = session_maker()
    scraped_data = ScrapedData(db_engine, db_session, config)
    return {
        "dotenv": dotenv,
        "config": config,
        "db_engine": db_engine,
        "db_session": db_session,
        "scraped_data": scraped_data,
    }
