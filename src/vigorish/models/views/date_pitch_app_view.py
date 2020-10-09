from sqlalchemy import func, join, select
from sqlalchemy_utils import create_view

from vigorish.config.database import Base, DateScrapeStatus, PitchAppScrapeStatus


class Date_PitchApp_View(Base):
    __table__ = create_view(
        name="date_pitch_app_status",
        selectable=select(
            [
                DateScrapeStatus.id.label("id"),
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
                DateScrapeStatus,
                PitchAppScrapeStatus,
                DateScrapeStatus.id == PitchAppScrapeStatus.scrape_status_date_id,
                isouter=True,
            )
        )
        .group_by(DateScrapeStatus.id),
        metadata=Base.metadata,
        cascade_on_drop=False,
    )
