"""Configuration of runners on base before a plate appearance begins."""
from sqlalchemy import Boolean, Column, Integer, String

from vigorish.config.database import Base


class RunnersOnBase(Base):
    """Configuration of runners on base before a plate appearance begins."""

    __tablename__ = "rob"
    id = Column(Integer, primary_key=True)
    runner_on_1b = Column(Boolean)
    runner_on_2b = Column(Boolean)
    runner_on_3b = Column(Boolean)
    scoring_position = Column(Boolean)
    base_open = Column(Boolean)
    dp_possible = Column(Boolean)
    notation = Column(String)

    def __str__(self):
        return self.notation

    def __repr__(self):
        return f"<RunnersOnBase {self.notation}>"

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    @classmethod
    def find_by_notation(cls, db_session, notation):
        return db_session.query(cls).filter_by(notation=notation).first()
