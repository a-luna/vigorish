from sqlalchemy import Column, DDL, event, MetaData, Table
from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement, PrimaryKeyConstraint


class CreateMaterializedView(DDLElement):
    def __init__(self, name, selectable):
        self.name = name
        self.selectable = selectable

@compiler.compiles(CreateMaterializedView)
def compile(element, compiler, **kw):
    # Could use "CREATE OR REPLACE MATERIALIZED VIEW..."
    sql = compiler.sql_compiler.process(element.selectable, literal_binds=True)
    return f'CREATE MATERIALIZED VIEW {element.name} AS {sql}'

def create_mat_view(metadata, name, selectable):
    _mt = MetaData()  # temp metadata just for initial Table object creation
    t = Table(name, _mt)  # the actual mat view class is bound to db.metadata
    for c in selectable.c:
        t.append_column(Column(c.name, c.type, primary_key=c.primary_key))

    if not (any(c.primary_key for c in selectable.c)):
        t.append_constraint(PrimaryKeyConstraint(*[c.name for c in selectable.c]))

    event.listen(
        metadata, "after_create",
        CreateMaterializedView(name, selectable)
    )

    @event.listens_for(metadata, "after_create")
    def create_indexes(target, connection, **kw):
        for idx in t.indexes:
            idx.create(connection)

    event.listen(
        metadata, "before_drop",
        DDL('DROP MATERIALIZED VIEW IF EXISTS ' + name)
    )
    return t
