from sqlalchemy import Column, Boolean, Integer, String, DateTime, ForeignKey
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from app.main.models.base import Base

class PitchAppearanceScrapeStatus(Base):

    __tablename__ = 'scrape_status_pitch_app'
    id = Column(Integer, primary_key=True)
    pitcher_id_mlb = Column(Integer)
    pitch_app_id = Column(String)
    bbref_game_id = Column(String)
    bb_game_id = Column(String)
    scraped_pitchfx = Column(Integer, default=0)
    no_pitchfx_data = Column(Integer, default=0)
    pitch_count_pitch_log = Column(Integer, default=0)
    pitch_count_pitchfx = Column(Integer, default=0)
    scrape_status_game_id = Column(Integer, ForeignKey("scrape_status_game.id"))
    scrape_status_date_id = Column(Integer, ForeignKey("scrape_status_date.id"))
    season_id = Column(Integer, ForeignKey("season.id"))

    def __repr__(self):
        return f'<PitchAppearanceScrapeStatus pitch_app_id="{self.pitch_app_id}">'

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def display(self):
        season_dict = self.as_dict()
        season_dict['game_date_time'] = self.game_date_time_str
        title = f"SCRAPE STATUS FOR PITCH APPEARANCE: {self.pitch_app_id}"
        display_dict(season_dict, title=title)

    @hybrid_property
    def bbref_pitch_app_id(self):
        return f"{self.bbref_game_id}_{self.pitcher_id_mlb}"

    @classmethod
    def find_by_pitch_app_id(cls, session, pitch_app_id):
        return session.query(cls).filter_by(pitch_app_id=pitch_app_id).first()

    @classmethod
    def find_by_bbref_pitch_app_id(cls, session, bbref_pitch_app_id):
        bbref_game_id = bbref_pitch_app_id.split("_")[0]
        pitcher_id_mlb = int(bbref_pitch_app_id.split("_")[1])
        return session.query(cls)\
            .filter_by(bbref_game_id=bbref_game_id)\
            .filter_by(pitcher_id_mlb=pitcher_id_mlb).first()

    @classmethod
    def get_all_scraped_pitch_app_ids_for_season(cls, session, season_id):
        return [
            pitch_app_status.pitch_app_id for pitch_app_status
            in session.query(cls).filter_by(season_id=season_id).all()
            if pitch_app_status.scraped_pitchfx == 1]

    @classmethod
    def get_all_unscraped_pitch_app_ids_for_season(cls, session, season_id):
        return [
            pitch_app_status.pitch_app_id for pitch_app_status
            in session.query(cls).filter_by(season_id=season_id).all()
            if pitch_app_status.scraped_pitchfx == 0]

    @classmethod
    def get_pitch_app_ids_without_pitchfx_data(cls, session, season_id):
        return [
            pitch_app_status.pitch_app_id for pitch_app_status
            in session.query(cls).filter_by(season_id=season_id).all()
            if pitch_app_status.no_pitchfx_data == 1]

    @classmethod
    def get_all_pitch_app_ids(cls, session, season_id):
        return [
            pitch_app_status.pitch_app_id
            for pitch_app_status
            in session.query(cls).filter_by(season_id=season_id).all()]

    @classmethod
    def get_all_bbref_pitch_app_ids_for_game(cls, session, bbref_game_id):
        return [
            pitch_app_status.bbref_pitch_app_id
            for pitch_app_status
            in session.query(cls).filter_by(bbref_game_id=bbref_game_id).all()
        ]

    @classmethod
    def get_all_unscraped_bbref_pitch_app_ids_for_game(cls, session, bbref_game_id):
        return [
            pitch_app_status.bbref_pitch_app_id
            for pitch_app_status
            in session.query(cls).filter_by(bbref_game_id=bbref_game_id).all()
            if pitch_app_status.scraped_pitchfx == 0
        ]

    @classmethod
    def get_all_scraped_bbref_pitch_app_ids_for_game(cls, session, bbref_game_id):
        return [
            pitch_app_status.bbref_pitch_app_id
            for pitch_app_status
            in session.query(cls).filter_by(bbref_game_id=bbref_game_id).all()
            if pitch_app_status.scraped_pitchfx == 1
        ]

    @classmethod
    def get_pitch_app_id_dict_from_pitch_app_guids(cls, session, pitch_app_guids):
        return {
            pfx.bbref_pitch_app_id:pfx.pitch_app_id
            for pfx in session.query(cls).all()
            if pfx.pitch_app_id in pitch_app_guids
        }
