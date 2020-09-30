"""Configuration of runners on base before a plate appearance begins."""
from sqlalchemy import Column, Boolean, Integer, String

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
        runners_on = []
        if self.runner_on_1b:
            runners_on.append("1B")
        if self.runner_on_2b:
            runners_on.append("2B")
        if self.runner_on_3b:
            runners_on.append("3B")
        if not runners_on:
            return "bases are empty"
        return f'Runners on: {", ".join(runners_on)}'

    def __repr__(self):
        return f"<RunnersOnBase {self.notation}>"

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    @classmethod
    def find_by_notation(cls, db_session, notation):
        return db_session.query(cls).filter_by(notation=notation).first()
