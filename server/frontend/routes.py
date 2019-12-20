"""URL route definitions for user interface pages."""
import random
from pprint import pformat
from http import HTTPStatus

from flask import send_from_directory, abort, jsonify, current_app

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


@frontend.route("/season/<year>")
def get_season_stats(year):
    session = current_app.session
    season = Season.find_by_year(session, year)
    if not season:
        abort(HTTPStatus.NOT_FOUND)
    return pformat(season.as_dict())
