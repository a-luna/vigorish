"""Flask app initialization via factory pattern."""
from pathlib import Path

from flask import Flask


APP_FOLDER = Path(__file__).resolve().parent.parent

def create_app():
    app = Flask("vigorish-app", root_path=APP_FOLDER)

    from server.frontend import frontend
    app.register_blueprint(frontend)

    return app
