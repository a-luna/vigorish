from __future__ import annotations

from dataclasses import dataclass

from dataclass_csv import accept_whitespaces
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

import vigorish.database as db


class PitchAppScrapeStatus(db.Base):
    __tablename__ = "scrape_status_pitch_app"
    id = Column(Integer, primary_key=True)
    pitcher_id_mlb = Column(Integer)
    pitch_app_id = Column(String, unique=True)
    bbref_game_id = Column(String)
    bb_game_id = Column(String)
    scraped_pitchfx = Column(Integer, default=0)
    no_pitchfx_data = Column(Integer, default=0)
    combined_pitchfx_bbref_data = Column(Integer, default=0)
    imported_pitchfx = Column(Integer, default=0)
    invalid_pitchfx = Column(Integer, default=0)
    pitchfx_error = Column(Integer, default=0)
    pitch_count_pitch_log = Column(Integer, default=0)
    pitch_count_bbref = Column(Integer, default=0)
    pitch_count_pitchfx = Column(Integer, default=0)
    pitch_count_pitchfx_audited = Column(Integer, default=0)
    batters_faced_bbref = Column(Integer, default=0)
    batters_faced_pitchfx = Column(Integer, default=0)
    patched_pitchfx_count = Column(Integer, default=0)
    missing_pitchfx_count = Column(Integer, default=0)
    removed_pitchfx_count = Column(Integer, default=0)
    invalid_pitchfx_count = Column(Integer, default=0)
    total_at_bats_pitchfx_complete = Column(Integer, default=0)
    total_at_bats_patched_pitchfx = Column(Integer, default=0)
    total_at_bats_missing_pitchfx = Column(Integer, default=0)
    total_at_bats_removed_pitchfx = Column(Integer, default=0)
    total_at_bats_pitchfx_error = Column(Integer, default=0)
    total_at_bats_invalid_pitchfx = Column(Integer, default=0)

    pitcher_id = Column(Integer, ForeignKey("player.id"), index=True)
    scrape_status_game_id = Column(Integer, ForeignKey("scrape_status_game.id"), index=True)
    scrape_status_date_id = Column(Integer, ForeignKey("scrape_status_date.id"))
    season_id = Column(Integer, ForeignKey("season.id"))

    pitchfx = relationship("PitchFx")

    @hybrid_property
    def game_date(self):
        return self.date.game_date

    @hybrid_property
    def contains_only_patched_data(self):
        return self.patched_pitchfx_count > 0 and (self.pitch_count_pitchfx_audited == self.patched_pitchfx_count)

    def __repr__(self):
        return f'<PitchAppScrapeStatus pitch_app_id="{self.pitch_app_id}">'

    @classmethod
    def find_by_pitch_app_id(cls, db_session, pitch_app_id) -> PitchAppScrapeStatus:
        return db_session.query(cls).filter_by(pitch_app_id=pitch_app_id).first()

    @classmethod
    def get_all_scraped_pitch_app_ids_for_season(cls, db_session, season_id):
        return [
            pitch_app_status.pitch_app_id
            for pitch_app_status in db_session.query(cls).filter_by(season_id=season_id).all()
            if pitch_app_status.scraped_pitchfx == 1
        ]

    @classmethod
    def get_all_pitch_app_ids_for_game(cls, db_session, bbref_game_id):
        return [
            pitch_app_status.pitch_app_id
            for pitch_app_status in db_session.query(cls).filter_by(bbref_game_id=bbref_game_id).all()
        ]

    @classmethod
    def get_all_pitch_app_ids_with_pfx_data_for_game(cls, db_session, bbref_game_id):
        pitch_apps_with_pfx_data = list(
            db_session.query(cls)
            .filter_by(bbref_game_id=bbref_game_id)
            .filter_by(scraped_pitchfx=1)
            .filter_by(no_pitchfx_data=0)
            .all()
        )

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

    @classmethod
    def get_pitch_app_id_map(cls, db_session):
        all_pitch_apps = db_session.query(cls).all()
        return {pa.pitch_app_id: pa.id for pa in all_pitch_apps}


@accept_whitespaces
@dataclass
class PitchAppScrapeStatusCsvRow:
    id: int
    pitcher_id_mlb: int = 0
    pitch_app_id: str = None
    bbref_game_id: str = None
    bb_game_id: str = None
    scraped_pitchfx: int = 0
    no_pitchfx_data: int = 0
    combined_pitchfx_bbref_data: int = 0
    imported_pitchfx: int = 0
    invalid_pitchfx: int = 0
    pitchfx_error: int = 0
    pitch_count_pitch_log: int = 0
    pitch_count_bbref: int = 0
    pitch_count_pitchfx: int = 0
    pitch_count_pitchfx_audited: int = 0
    batters_faced_bbref: int = 0
    batters_faced_pitchfx: int = 0
    patched_pitchfx_count: int = 0
    missing_pitchfx_count: int = 0
    removed_pitchfx_count: int = 0
    invalid_pitchfx_count: int = 0
    total_at_bats_pitchfx_complete: int = 0
    total_at_bats_patched_pitchfx: int = 0
    total_at_bats_missing_pitchfx: int = 0
    total_at_bats_removed_pitchfx: int = 0
    total_at_bats_pitchfx_error: int = 0
    total_at_bats_invalid_pitchfx: int = 0
