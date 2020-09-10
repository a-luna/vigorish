from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.hybrid import hybrid_property

from vigorish.config.database import Base
from vigorish.util.list_helpers import display_dict
from vigorish.util.dt_format_strings import DATE_ONLY_TABLE_ID


class PitchAppScrapeStatus(Base):

    __tablename__ = "scrape_status_pitch_app"
    id = Column(Integer, primary_key=True)
    pitcher_id_mlb = Column(Integer)
    pitch_app_id = Column(String, unique=True)
    bbref_game_id = Column(String)
    bb_game_id = Column(String)
    scraped_pitchfx = Column(Integer, default=0)
    no_pitchfx_data = Column(Integer, default=0)
    combined_pitchfx_bbref_data = Column(Integer, default=0)
    batters_faced_bbref = Column(Integer, default=0)
    batters_faced_pitchfx = Column(Integer, default=0)
    duplicate_guid_removed_count = Column(Integer, default=0)
    pitch_count_pitch_log = Column(Integer, default=0)
    pitch_count_bbref = Column(Integer, default=0)
    pitch_count_pitchfx = Column(Integer, default=0)
    pitch_count_pitchfx_audited = Column(Integer, default=0)
    total_at_bats_pitchfx_complete = Column(Integer, default=0)
    patched_pitchfx_count = Column(Integer, default=0)
    total_at_bats_patched_pitchfx = Column(Integer, default=0)
    missing_pitchfx_count = Column(Integer, default=0)
    total_at_bats_missing_pitchfx = Column(Integer, default=0)
    extra_pitchfx_count = Column(Integer, default=0)
    total_at_bats_extra_pitchfx = Column(Integer, default=0)
    extra_pitchfx_removed_count = Column(Integer, default=0)
    total_at_bats_extra_pitchfx_removed = Column(Integer, default=0)
    pitchfx_error = Column(Integer, default=0)
    total_at_bats_pitchfx_error = Column(Integer, default=0)
    invalid_pitchfx = Column(Integer, default=0)
    invalid_pitchfx_count = Column(Integer, default=0)
    total_at_bats_invalid_pitchfx = Column(Integer, default=0)
    scrape_status_game_id = Column(Integer, ForeignKey("scrape_status_game.id"))
    scrape_status_date_id = Column(Integer, ForeignKey("scrape_status_date.id"))
    season_id = Column(Integer, ForeignKey("season.id"))

    @hybrid_property
    def contains_patched_data(self):
        return self.patched_pitchfx_count > 0

    @hybrid_property
    def contains_only_patched_data(self):
        return self.patched_pitchfx_count > 0 and (
            self.pitch_count_pitchfx_audited == self.patched_pitchfx_count
        )

    def __repr__(self):
        return f'<PitchAppScrapeStatus pitch_app_id="{self.pitch_app_id}">'

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def display(self):
        title = f"SCRAPE STATUS FOR PITCH APPEARANCE: {self.pitch_app_id}"
        display_dict(self.as_dict(), title=title)

    @classmethod
    def find_by_pitch_app_id(cls, db_session, pitch_app_id):
        return db_session.query(cls).filter_by(pitch_app_id=pitch_app_id).first()

    @classmethod
    def get_all_scraped_pitch_app_ids_for_season(cls, db_session, season_id):
        return [
            pitch_app_status.pitch_app_id
            for pitch_app_status in db_session.query(cls).filter_by(season_id=season_id).all()
            if pitch_app_status.scraped_pitchfx == 1
        ]

    @classmethod
    def get_all_unscraped_pitch_app_ids_for_season(cls, db_session, season_id):
        return [
            pitch_app_status.pitch_app_id
            for pitch_app_status in db_session.query(cls).filter_by(season_id=season_id).all()
            if pitch_app_status.scraped_pitchfx == 0
        ]

    @classmethod
    def get_pitch_app_ids_without_pitchfx_data(cls, db_session, season_id):
        return [
            pitch_app_status.pitch_app_id
            for pitch_app_status in db_session.query(cls).filter_by(season_id=season_id).all()
            if pitch_app_status.no_pitchfx_data == 1
        ]

    @classmethod
    def get_all_pitch_app_ids(cls, db_session, season_id):
        return [
            pitch_app_status.pitch_app_id
            for pitch_app_status in db_session.query(cls).filter_by(season_id=season_id).all()
        ]

    @classmethod
    def get_all_pitch_app_ids_for_date(cls, db_session, date):
        date_id = date.strftime(DATE_ONLY_TABLE_ID)
        return [
            pitch_app.pitch_app_id
            for pitch_app in db_session.query(cls)
            .filter_by(scrape_status_date_id=int(date_id))
            .all()
        ]

    @classmethod
    def get_all_pitch_app_ids_for_game(cls, db_session, bbref_game_id):
        return [
            pitch_app_status.pitch_app_id
            for pitch_app_status in db_session.query(cls)
            .filter_by(bbref_game_id=bbref_game_id)
            .all()
        ]

    @classmethod
    def get_all_scraped_pitch_app_ids_for_game_with_pitchfx_data(cls, db_session, bbref_game_id):
        pitch_apps_with_pfx_data = [
            pitch_app_status
            for pitch_app_status in db_session.query(cls)
            .filter_by(bbref_game_id=bbref_game_id)
            .filter_by(scraped_pitchfx=1)
            .filter_by(no_pitchfx_data=0)
            .all()
        ]
        return [
            pitch_app_status.pitch_app_id
            for pitch_app_status in pitch_apps_with_pfx_data
            if not pitch_app_status.contains_only_patched_data
        ]

    @classmethod
    def get_all_unscraped_pitch_app_ids_for_game(cls, db_session, bbref_game_id):
        return [
            pitch_app_status.pitch_app_id
            for pitch_app_status in db_session.query(cls)
            .filter_by(bbref_game_id=bbref_game_id)
            .filter_by(scraped_pitchfx=0)
            .all()
        ]

    @classmethod
    def get_all_scraped_pitch_app_ids_for_game(cls, db_session, bbref_game_id):
        return [
            pitch_app_status.pitch_app_id
            for pitch_app_status in db_session.query(cls)
            .filter_by(bbref_game_id=bbref_game_id)
            .filter_by(scraped_pitchfx=1)
            .all()
        ]
