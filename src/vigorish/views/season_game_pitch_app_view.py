from sqlalchemy import and_, func, join, select
from sqlalchemy_utils import create_view

import vigorish.database as db
from vigorish.enums import SeasonType
from vigorish.util.list_helpers import flatten_list2d


class Season_Game_PitchApp_View(db.Base):
    __table__ = create_view(
        name="season_game_pitch_app_status",
        selectable=select(
            [
                db.Season.id.label("id"),
                db.Season.year.label("year"),
                db.Season.season_type.label("season_type"),
                db.PitchAppScrapeStatus.scrape_status_date_id.label("date_id"),
                db.PitchAppScrapeStatus.scrape_status_game_id.label("game_id"),
                db.PitchAppScrapeStatus.bbref_game_id.label("bbref_game_id"),
                func.count(db.PitchAppScrapeStatus.id).label("total_pitchfx"),
                func.sum(db.PitchAppScrapeStatus.scraped_pitchfx).label("total_pitchfx_scraped"),
                func.sum(db.PitchAppScrapeStatus.no_pitchfx_data).label("total_no_pitchfx_data"),
                func.sum(db.PitchAppScrapeStatus.combined_pitchfx_bbref_data).label(
                    "total_combined_pitchfx_bbref_data"
                ),
                func.sum(db.PitchAppScrapeStatus.batters_faced_bbref).label("total_batters_faced_bbref"),
                func.sum(db.PitchAppScrapeStatus.batters_faced_pitchfx).label("total_batters_faced_pitchfx"),
                func.sum(db.PitchAppScrapeStatus.pitch_count_pitch_log).label("total_pitch_count_pitch_log"),
                func.sum(db.PitchAppScrapeStatus.pitch_count_bbref).label("total_pitch_count_bbref"),
                func.sum(db.PitchAppScrapeStatus.pitch_count_pitchfx).label("total_pitch_count_pitchfx"),
                func.sum(db.PitchAppScrapeStatus.pitch_count_pitchfx_audited).label(
                    "total_pitch_count_pitchfx_audited"
                ),
                func.sum(db.PitchAppScrapeStatus.total_at_bats_pitchfx_complete).label(
                    "total_at_bats_pitchfx_complete"
                ),
                func.sum(db.PitchAppScrapeStatus.patched_pitchfx_count).label("total_patched_pitchfx_count"),
                func.sum(db.PitchAppScrapeStatus.total_at_bats_patched_pitchfx).label("total_at_bats_patched_pitchfx"),
                func.sum(db.PitchAppScrapeStatus.missing_pitchfx_count).label("total_missing_pitchfx_count"),
                func.sum(db.PitchAppScrapeStatus.total_at_bats_missing_pitchfx).label("total_at_bats_missing_pitchfx"),
                func.sum(db.PitchAppScrapeStatus.removed_pitchfx_count).label("total_removed_pitchfx_count"),
                func.sum(db.PitchAppScrapeStatus.total_at_bats_removed_pitchfx).label("total_at_bats_removed_pitchfx"),
                func.sum(db.PitchAppScrapeStatus.pitchfx_error).label("total_pitchfx_error"),
                func.sum(db.PitchAppScrapeStatus.total_at_bats_pitchfx_error).label("total_at_bats_pitchfx_error"),
                func.sum(db.PitchAppScrapeStatus.invalid_pitchfx).label("total_invalid_pitchfx"),
                func.sum(db.PitchAppScrapeStatus.invalid_pitchfx_count).label("total_invalid_pitchfx_count"),
                func.sum(db.PitchAppScrapeStatus.total_at_bats_invalid_pitchfx).label("total_at_bats_invalid_pitchfx"),
            ]
        )
        .select_from(
            join(
                db.Season,
                db.PitchAppScrapeStatus,
                db.Season.id == db.PitchAppScrapeStatus.season_id,
            )
        )
        .group_by(db.PitchAppScrapeStatus.scrape_status_game_id),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_all_bbref_game_ids_eligible_for_audit(cls, db_engine, year, season_type=SeasonType.REGULAR_SEASON):
        s = select([cls.bbref_game_id]).where(
            and_(
                cls.year == year,
                cls.season_type == season_type,
                cls.total_pitchfx == cls.total_pitchfx_scraped,
                cls.total_pitchfx != cls.total_combined_pitchfx_bbref_data,
            )
        )
        results = db_engine.execute(s).fetchall()
        return flatten_list2d([d.values() for d in [dict(row) for row in results]])

    @classmethod
    def get_all_bbref_game_ids_all_pitchfx_logs_are_valid(cls, db_engine, year, season_type=SeasonType.REGULAR_SEASON):
        s = select([cls.bbref_game_id]).where(
            and_(
                cls.year == year,
                cls.season_type == season_type,
                cls.total_pitchfx == cls.total_combined_pitchfx_bbref_data,
                cls.total_pitchfx_error == 0,
                cls.total_invalid_pitchfx == 0,
            )
        )
        results = db_engine.execute(s).fetchall()
        return flatten_list2d([d.values() for d in [dict(row) for row in results]])

    @classmethod
    def get_all_bbref_game_ids_pitchfx_error(cls, db_engine, year, season_type=SeasonType.REGULAR_SEASON):
        s = select([cls.bbref_game_id]).where(
            and_(
                cls.year == year,
                cls.season_type == season_type,
                cls.total_pitchfx_error > 0,
            )
        )
        results = db_engine.execute(s).fetchall()
        return flatten_list2d([d.values() for d in [dict(row) for row in results]])

    @classmethod
    def get_all_bbref_game_ids_invalid_pitchfx(cls, db_engine, year, season_type=SeasonType.REGULAR_SEASON):
        s = select([cls.bbref_game_id]).where(
            and_(
                cls.year == year,
                cls.season_type == season_type,
                cls.total_invalid_pitchfx > 0,
            )
        )
        results = db_engine.execute(s).fetchall()
        return flatten_list2d([d.values() for d in [dict(row) for row in results]])

    @classmethod
    def get_all_bbref_game_ids_combined_no_missing_pfx(cls, db_engine, season_type=SeasonType.REGULAR_SEASON):
        s = select([cls.bbref_game_id]).where(
            and_(
                cls.season_type == season_type,
                cls.total_pitchfx == cls.total_combined_pitchfx_bbref_data,
                cls.total_missing_pitchfx_count == 0,
                cls.total_pitchfx_error == 0,
                cls.total_invalid_pitchfx == 0,
            )
        )
        results = db_engine.execute(s).fetchall()
        return flatten_list2d([d.values() for d in [dict(row) for row in results]])
