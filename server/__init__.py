"""Flask app initialization via factory pattern."""
import os
from pathlib import Path

from flask import Flask
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


APP_FOLDER = Path(__file__).resolve().parent.parent

def create_app():
    app = Flask("vigorish-app", root_path=APP_FOLDER)

    from server.frontend import frontend
    app.register_blueprint(frontend)

    engine = create_engine(os.getenv("DATABASE_URL"))
    session_maker = sessionmaker(bind=engine)
    app.session = session_maker()

    return app
