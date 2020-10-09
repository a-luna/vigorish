from sqlalchemy import func, join, select
from sqlalchemy_utils import create_view

from vigorish.config.database import Base, DateScrapeStatus, Season


class Season_Date_View(Base):
    __table__ = create_view(
        name="season_date_status",
        selectable=select(
            [
                Season.id.label("id"),
                func.count(DateScrapeStatus.id).label("total_days"),
                func.sum(DateScrapeStatus.scraped_daily_dash_bbref).label(
                    "total_scraped_daily_dash_bbref"
                ),
                func.sum(DateScrapeStatus.scraped_daily_dash_brooks).label(
                    "total_scraped_daily_dash_brooks"
                ),
                func.sum(DateScrapeStatus.game_count_bbref).label("total_game_count_bbref"),
                func.sum(DateScrapeStatus.game_count_brooks).label("total_game_count_brooks"),
            ]
        )
        .select_from(
            join(
                Season,
                DateScrapeStatus,
                Season.id == DateScrapeStatus.season_id,
                isouter=True,
            )
        )
        .group_by(Season.id),
        metadata=Base.metadata,
        cascade_on_drop=False,
    )
