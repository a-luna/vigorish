from flask import Blueprint

frontend = Blueprint('frontend', __name__, url_prefix='/')

from server.frontend import routes
