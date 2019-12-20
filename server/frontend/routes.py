"""URL route definitions for user interface pages."""
import random
from http import HTTPStatus

from flask import send_from_directory, abort, jsonify

from app.main.models.season import Season
from server.frontend import frontend

# Path for our main Svelte page
@frontend.route("/")
def base():
    return send_from_directory('client/public', 'index.html')

# Path for all the static files (compiled JS/CSS, etc.)
@frontend.route("/<path:path>")
def home(path):
    return send_from_directory('client/public', path)


@frontend.route("/season/<int:year>")
def hello(year):
    season = Season.find_by_year(year)
    if not season:
        abort(HTTPStatus.NOT_FOUND)
    return jsonify(season.as_dict())
