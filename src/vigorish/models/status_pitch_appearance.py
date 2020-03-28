from sqlalchemy import Column, Boolean, Integer, String, DateTime, ForeignKey
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from vigorish.config.database import Base
from vigorish.util.list_helpers import display_dict
from vigorish.util.dt_format_strings import DATE_ONLY_TABLE_ID


class PitchAppearanceScrapeStatus(Base):

    __tablename__ = "scrape_status_pitch_app"
    id = Column(Integer, primary_key=True)
    pitcher_id_mlb = Column(Integer)
    pitch_app_id = Column(String)
    bbref_game_id = Column(String)
    bb_game_id = Column(String)
    scraped_pitchfx = Column(Integer, default=0)
    no_pitchfx_data = Column(Integer, default=0)
    pitch_count_pitch_log = Column(Integer, default=0)
    pitch_count_pitchfx = Column(Integer, default=0)
    pitch_app_audited = Column(Integer, default=0)
    pitch_count_pitchfx_audited = Column(Integer, default=0)
    pitch_count_bbref = Column(Integer, default=0)
    duplicate_pitchfx_removed_count = Column(Integer, default=0)
    missing_pitchfx_is_valid = Column(Integer, default=0)
    scrape_status_game_id = Column(Integer, ForeignKey("scrape_status_game.id"))
    scrape_status_date_id = Column(Integer, ForeignKey("scrape_status_date.id"))
    season_id = Column(Integer, ForeignKey("season.id"))

    def __repr__(self):
        return f'<PitchAppearanceScrapeStatus pitch_app_id="{self.pitch_app_id}">'

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def display(self):
        season_dict = self.as_dict()
        title = f"SCRAPE STATUS FOR PITCH APPEARANCE: {self.pitch_app_id}"
        display_dict(season_dict, title=title)

    @classmethod
    def find_by_pitch_app_id(cls, session, pitch_app_id):
        return session.query(cls).filter_by(pitch_app_id=pitch_app_id).first()

    @classmethod
    def get_all_scraped_pitch_app_ids_for_season(cls, session, season_id):
        return [
            pitch_app_status.pitch_app_id
            for pitch_app_status in session.query(cls).filter_by(season_id=season_id).all()
            if pitch_app_status.scraped_pitchfx == 1
        ]

    @classmethod
    def get_all_unscraped_pitch_app_ids_for_season(cls, session, season_id):
        return [
            pitch_app_status.pitch_app_id
            for pitch_app_status in session.query(cls).filter_by(season_id=season_id).all()
            if pitch_app_status.scraped_pitchfx == 0
        ]

    @classmethod
    def get_pitch_app_ids_without_pitchfx_data(cls, session, season_id):
        return [
            pitch_app_status.pitch_app_id
            for pitch_app_status in session.query(cls).filter_by(season_id=season_id).all()
            if pitch_app_status.no_pitchfx_data == 1
        ]

    @classmethod
    def get_all_pitch_app_ids(cls, session, season_id):
        return [
            pitch_app_status.pitch_app_id
            for pitch_app_status in session.query(cls).filter_by(season_id=season_id).all()
        ]

    @classmethod
    def get_all_pitch_app_ids_for_date(cls, session, date):
        date_id = date.strftime(DATE_ONLY_TABLE_ID)
        return [
            pitch_app.pitch_app_id
            for pitch_app in session.query(cls).filter_by(scrape_status_date_id=int(date_id)).all()
        ]

    @classmethod
    def get_all_pitch_app_ids_for_game(cls, session, bbref_game_id):
        return [
            pitch_app_status.pitch_app_id
            for pitch_app_status in session.query(cls).filter_by(bbref_game_id=bbref_game_id).all()
        ]

    @classmethod
    def get_all_pitch_app_ids_for_game_with_pitchfx_data(cls, session, bbref_game_id):
        return [
            pitch_app_status.pitch_app_id
            for pitch_app_status in session.query(cls).filter_by(bbref_game_id=bbref_game_id).all()
            if not pitch_app_status.no_pitchfx_data
        ]

    @classmethod
    def get_all_unscraped_pitch_app_ids_for_game(cls, session, bbref_game_id):
        return [
            pitch_app_status.pitch_app_id
            for pitch_app_status in session.query(cls)
            .filter_by(bbref_game_id=bbref_game_id)
            .filter_by(scraped_pitchfx=0)
            .all()
        ]

    @classmethod
    def get_all_scraped_pitch_app_ids_for_game(cls, session, bbref_game_id):
        return [
            pitch_app_status.pitch_app_id
            for pitch_app_status in session.query(cls)
            .filter_by(bbref_game_id=bbref_game_id)
            .filter_by(scraped_pitchfx=1)
            .all()
        ]