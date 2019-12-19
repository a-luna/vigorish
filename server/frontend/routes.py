"""URL route definitions for user interface pages."""
import random

from flask import send_from_directory

from server.frontend import frontend

# Path for our main Svelte page
@frontend.route("/")
def base():
    return send_from_directory('client/public', 'index.html')

# Path for all the static files (compiled JS/CSS, etc.)
@frontend.route("/<path:path>")
def home(path):
    return send_from_directory('client/public', path)


@frontend.route("/rand")
def hello():
    return str(random.randint(0, 100))
