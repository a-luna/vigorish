from sqlalchemy import Column, DateTime, Float, Integer

from vigorish.config.database import Base
from vigorish.util.datetime_util import utc_now


class TimeBetweenPitches(Base):

    __tablename__ = "time_between_pitches"
    id = Column(Integer, primary_key=True)
    between_pitches_sample_size = Column(Integer, default=0)
    between_pitches_total_sec = Column(Integer, default=0)
    between_pitches_avg_sec = Column(Float, default=0.0)
    between_pitches_min_sec = Column(Float, default=0.0)
    between_pitches_max_sec = Column(Float, default=0.0)
    between_at_bats_sample_size = Column(Integer, default=0)
    between_at_bats_total_sec = Column(Integer, default=0)
    between_at_bats_avg_sec = Column(Float, default=0.0)
    between_at_bats_min_sec = Column(Float, default=0.0)
    between_at_bats_max_sec = Column(Float, default=0.0)
    between_innings_sample_size = Column(Integer, default=0)
    between_innings_total_sec = Column(Integer, default=0)
    between_innings_avg_sec = Column(Float, default=0.0)
    between_innings_min_sec = Column(Float, default=0.0)
    between_innings_max_sec = Column(Float, default=0.0)
    calc_timestamp = Column(DateTime, default=utc_now)

    def as_dict(self):
        return {
            "time_between_pitches": {
                "total": self.between_pitches_sample_size,
                "count": self.between_pitches_total_sec,
                "avg": self.between_pitches_avg_sec,
                "max": self.between_pitches_max_sec,
                "min": self.between_pitches_min_sec,
                "range": self.between_pitches_max_sec - self.between_pitches_min_sec,
            },
            "time_between_at_bats": {
                "total": self.between_at_bats_sample_size,
                "count": self.between_at_bats_total_sec,
                "avg": self.between_at_bats_avg_sec,
                "max": self.between_at_bats_max_sec,
                "min": self.between_at_bats_min_sec,
                "range": self.between_at_bats_max_sec - self.between_at_bats_min_sec,
            },
            "time_between_innings": {
                "total": self.between_innings_sample_size,
                "count": self.between_innings_total_sec,
                "avg": self.between_innings_avg_sec,
                "max": self.between_innings_max_sec,
                "min": self.between_innings_min_sec,
                "range": self.between_innings_max_sec - self.between_innings_min_sec,
            },
            "timestamp": self.calc_timestamp,
        }

    @classmethod
    def from_calc_results(cls, db_session, calc_results):
        between_pitches = calc_results["time_between_pitches"]
        between_at_bats = calc_results["time_between_at_bats"]
        between_innings = calc_results["time_between_innings"]
        add_results = {
            "between_pitches_sample_size": between_pitches["total"],
            "between_pitches_total_sec": between_pitches["count"],
            "between_pitches_avg_sec": between_pitches["avg"],
            "between_pitches_min_sec": between_pitches["min"],
            "between_pitches_max_sec": between_pitches["max"],
            "between_at_bats_sample_size": between_at_bats["total"],
            "between_at_bats_total_sec": between_at_bats["count"],
            "between_at_bats_avg_sec": between_at_bats["avg"],
            "between_at_bats_min_sec": between_at_bats["min"],
            "between_at_bats_max_sec": between_at_bats["max"],
            "between_innings_sample_size": between_innings["total"],
            "between_innings_total_sec": between_innings["count"],
            "between_innings_avg_sec": between_innings["avg"],
            "between_innings_min_sec": between_innings["min"],
            "between_innings_max_sec": between_innings["max"],
        }
        time_between_pitches = cls(**add_results)
        db_session.add(time_between_pitches)
        db_session.commit()
        return time_between_pitches

    @classmethod
    def get_latest_results(cls, db_session):
        results = db_session.query(cls).all()
        if not results:
            return None
        results.sort(key=lambda x: x.calc_timestamp, reverse=True)
        return results[0].as_dict()
