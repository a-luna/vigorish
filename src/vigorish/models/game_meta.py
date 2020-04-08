"""Db model that describes game info such as date/time, weather, location, etc."""
from datetime import datetime
from dateutil import tz

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.ext.hybrid import hybrid_property

from vigorish.config.database import Base
from vigorish.util.list_helpers import display_dict


class GameMetaInformation(Base):
    """Db model that describes game info such as date/time, weather, location, etc."""

    __tablename__ = "game_meta"
    id = Column(Integer, primary_key=True)
    game_date = Column(DateTime)
    game_time_hour = Column(Integer)
    game_time_minute = Column(Integer)
    game_time_zone = Column(String)
    park_name = Column(String)
    field_type = Column(String)
    day_night = Column(String)
    first_pitch_temperature = Column(String)
    first_pitch_precipitation = Column(String)
    first_pitch_wind = Column(String)
    first_pitch_clouds = Column(String)
    game_duration = Column(String)
    game_duration_minutes = Column(Integer)
    attendance = Column(Integer)
    bbref_game_id = Column(String)
    boxscore_id = Column(Integer, ForeignKey("boxscore.id"))

    @hybrid_property
    def game_start_time(self):
        return datetime(
            year=self.game_date.year,
            month=self.game_date.month,
            day=self.game_date.day,
            hour=self.game_time_hour,
            minute=self.game_time_minute,
            tzinfo=tz.gettz(self.game_time_zone),
        )

    def __repr__(self):
        return f"<GameMetaInformation bbref_game_id={self.bbref_game_id}, id={self.id}>"

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def display(self):
        meta_dict = self.as_dict()
        title = f"Game meta info for {self.bbref_game_id}"
        display_dict(meta_dict, title=title)
