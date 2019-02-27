from sqlalchemy import inspect
from app.main.models.base import Base

class MaterializedView(Base):
    __abstract__ = True

    @classmethod
    def refresh(session, cls, concurrently=True):
        '''Refreshes the current materialized view'''
        # since session.execute() bypasses autoflush, must manually flush in order
        # to include newly-created/modified objects in the refresh
        refresh_mat_view(session, cls.__table__.fullname, concurrently)


def refresh_mat_view(session, name, concurrently):
    session.flush()
    concurrent = 'CONCURRENTLY ' if concurrently else ''
    session.execute(f'REFRESH MATERIALIZED VIEW {concurrent} {name}')

def refresh_all_mat_views(engine, session, concurrently=True):
    mat_views = inspect(engine).get_view_names()
    for v in mat_views:
        refresh_mat_view(session, v, concurrently)