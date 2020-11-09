from sqlalchemy import and_, func, join, select
from sqlalchemy_utils import create_view

from vigorish.config.database import Base, PitchAppScrapeStatus, Season
from vigorish.enums import SeasonType
from vigorish.util.list_helpers import flatten_list2d


class Season_Game_PitchApp_View(Base):
    __table__ = create_view(
        name="season_game_pitch_app_status",
        selectable=select(
            [
                Season.id.label("id"),
                Season.year.label("year"),
                Season.season_type.label("season_type"),
                PitchAppScrapeStatus.scrape_status_date_id.label("date_id"),
                PitchAppScrapeStatus.scrape_status_game_id.label("game_id"),
                PitchAppScrapeStatus.bbref_game_id.label("bbref_game_id"),
                func.count(PitchAppScrapeStatus.id).label("total_pitchfx"),
                func.sum(PitchAppScrapeStatus.scraped_pitchfx).label("total_pitchfx_scraped"),
                func.sum(PitchAppScrapeStatus.no_pitchfx_data).label("total_no_pitchfx_data"),
                func.sum(PitchAppScrapeStatus.combined_pitchfx_bbref_data).label(
                    "total_combined_pitchfx_bbref_data"
                ),
                func.sum(PitchAppScrapeStatus.batters_faced_bbref).label(
                    "total_batters_faced_bbref"
                ),
                func.sum(PitchAppScrapeStatus.batters_faced_pitchfx).label(
                    "total_batters_faced_pitchfx"
                ),
                func.sum(PitchAppScrapeStatus.duplicate_guid_removed_count).label(
                    "total_duplicate_pitchfx_removed_count"
                ),
                func.sum(PitchAppScrapeStatus.pitch_count_pitch_log).label(
                    "total_pitch_count_pitch_log"
                ),
                func.sum(PitchAppScrapeStatus.pitch_count_bbref).label("total_pitch_count_bbref"),
                func.sum(PitchAppScrapeStatus.pitch_count_pitchfx).label(
                    "total_pitch_count_pitchfx"
                ),
                func.sum(PitchAppScrapeStatus.pitch_count_pitchfx_audited).label(
                    "total_pitch_count_pitchfx_audited"
                ),
                func.sum(PitchAppScrapeStatus.patched_pitchfx_count).label(
                    "total_patched_pitchfx_count"
                ),
                func.sum(PitchAppScrapeStatus.total_at_bats_patched_pitchfx).label(
                    "total_at_bats_patched_pitchfx"
                ),
                func.sum(PitchAppScrapeStatus.missing_pitchfx_count).label(
                    "total_missing_pitchfx_count"
                ),
                func.sum(PitchAppScrapeStatus.total_at_bats_missing_pitchfx).label(
                    "total_at_bats_missing_pitchfx"
                ),
                func.sum(PitchAppScrapeStatus.extra_pitchfx_count).label(
                    "total_extra_pitchfx_count"
                ),
                func.sum(PitchAppScrapeStatus.total_at_bats_extra_pitchfx).label(
                    "total_at_bats_extra_pitchfx"
                ),
                func.sum(PitchAppScrapeStatus.extra_pitchfx_removed_count).label(
                    "total_extra_pitchfx_removed_count"
                ),
                func.sum(PitchAppScrapeStatus.total_at_bats_extra_pitchfx_removed).label(
                    "total_at_bats_extra_pitchfx_removed"
                ),
                func.sum(PitchAppScrapeStatus.pitchfx_error).label("total_pitchfx_error"),
                func.sum(PitchAppScrapeStatus.total_at_bats_pitchfx_error).label(
                    "total_at_bats_pitchfx_error"
                ),
                func.sum(PitchAppScrapeStatus.invalid_pitchfx).label("total_invalid_pitchfx"),
                func.sum(PitchAppScrapeStatus.invalid_pitchfx_count).label(
                    "total_invalid_pitchfx_count"
                ),
                func.sum(PitchAppScrapeStatus.total_at_bats_invalid_pitchfx).label(
                    "total_at_bats_invalid_pitchfx"
                ),
            ]
        )
        .select_from(
            join(
                Season,
                PitchAppScrapeStatus,
                Season.id == PitchAppScrapeStatus.season_id,
                isouter=True,
            )
        )
        .group_by(PitchAppScrapeStatus.scrape_status_game_id),
        metadata=Base.metadata,
        cascade_on_drop=False,
    )

    @classmethod
    def get_all_bbref_game_ids_no_pitchfx_data_for_any_pitch_apps(
        cls, db_engine, year, season_type=SeasonType.REGULAR_SEASON
    ):
        s = select([Season_Game_PitchApp_View.bbref_game_id]).where(
            and_(
                Season_Game_PitchApp_View.year == year,
                Season_Game_PitchApp_View.total_pitchfx
                == Season_Game_PitchApp_View.total_pitchfx_scraped,
                Season_Game_PitchApp_View.total_no_pitchfx_data > 0,
            )
        )
        results = db_engine.execute(s).fetchall()
        return flatten_list2d([d.values() for d in [dict(row) for row in results]])

    @classmethod
    def get_all_bbref_game_ids_eligible_for_audit(
        cls, db_engine, year, season_type=SeasonType.REGULAR_SEASON
    ):
        s = select([Season_Game_PitchApp_View.bbref_game_id]).where(
            and_(
                Season_Game_PitchApp_View.year == year,
                Season_Game_PitchApp_View.total_pitchfx
                == Season_Game_PitchApp_View.total_pitchfx_scraped,
                Season_Game_PitchApp_View.total_pitchfx
                != Season_Game_PitchApp_View.total_combined_pitchfx_bbref_data,
            )
        )
        results = db_engine.execute(s).fetchall()
        return flatten_list2d([d.values() for d in [dict(row) for row in results]])

    @classmethod
    def get_all_bbref_game_ids_all_pitchfx_logs_are_valid(
        cls, db_engine, year, season_type=SeasonType.REGULAR_SEASON
    ):
        s = select([Season_Game_PitchApp_View.bbref_game_id]).where(
            and_(
                Season_Game_PitchApp_View.year == year,
                Season_Game_PitchApp_View.total_pitchfx
                == Season_Game_PitchApp_View.total_combined_pitchfx_bbref_data,
                Season_Game_PitchApp_View.total_pitchfx_error == 0,
                Season_Game_PitchApp_View.total_invalid_pitchfx == 0,
            )
        )
        results = db_engine.execute(s).fetchall()
        return flatten_list2d([d.values() for d in [dict(row) for row in results]])

    @classmethod
    def get_all_bbref_game_ids_pitchfx_error(
        cls, db_engine, year, season_type=SeasonType.REGULAR_SEASON
    ):
        s = select([Season_Game_PitchApp_View.bbref_game_id]).where(
            and_(
                Season_Game_PitchApp_View.year == year,
                Season_Game_PitchApp_View.total_pitchfx_error > 0,
            )
        )
        results = db_engine.execute(s).fetchall()
        return flatten_list2d([d.values() for d in [dict(row) for row in results]])

    @classmethod
    def get_all_bbref_game_ids_invalid_pitchfx(
        cls, db_engine, year, season_type=SeasonType.REGULAR_SEASON
    ):
        s = select([Season_Game_PitchApp_View.bbref_game_id]).where(
            and_(
                Season_Game_PitchApp_View.year == year,
                Season_Game_PitchApp_View.total_invalid_pitchfx > 0,
            )
        )
        results = db_engine.execute(s).fetchall()
        return flatten_list2d([d.values() for d in [dict(row) for row in results]])

    @classmethod
    def get_all_bbref_game_ids_combined_no_missing_pfx(
        cls, db_engine, season_type=SeasonType.REGULAR_SEASON
    ):
        s = select([Season_Game_PitchApp_View.bbref_game_id]).where(
            and_(
                Season_Game_PitchApp_View.total_pitchfx
                == Season_Game_PitchApp_View.total_combined_pitchfx_bbref_data,
                Season_Game_PitchApp_View.total_missing_pitchfx_count == 0,
                Season_Game_PitchApp_View.total_pitchfx_error == 0,
                Season_Game_PitchApp_View.total_invalid_pitchfx == 0,
            )
        )
        results = db_engine.execute(s).fetchall()
        return flatten_list2d([d.values() for d in [dict(row) for row in results]])
