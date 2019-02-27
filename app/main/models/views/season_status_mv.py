from sqlalchemy import select, func
from app.main.models.base import Base
from app.main.models.views.materialized_view import MaterializedView
from app.main.util.materialized_view_factory import create_mat_view

class SeasonStatusMV(MaterializedView):
