"""Flask app initialization via factory pattern."""
import json
import os
from pathlib import Path

from flask import Flask
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


APP_FOLDER = Path(__file__).resolve().parent.parent
CONFIG_PATH = APP_FOLDER / "config.json"

def create_app():
    app = Flask("vigorish-app", root_path=APP_FOLDER)
    config = json.loads(CONFIG_PATH.read_text())
    engine = create_engine(config.get("DATABASE_URL"))
    session_maker = sessionmaker(bind=engine)
    app.session = session_maker()

    from server.frontend import frontend
    app.register_blueprint(frontend)

    return app
