from sqlalchemy import func, join, select
from sqlalchemy_utils import create_view

import vigorish.database as db


class Season_Date_View(db.Base):
    __table__ = create_view(
        name="season_date_status",
        selectable=select(
            [
                db.Season.id.label("id"),
                func.count(db.DateScrapeStatus.id).label("total_days"),
                func.sum(db.DateScrapeStatus.scraped_daily_dash_bbref).label("total_scraped_daily_dash_bbref"),
                func.sum(db.DateScrapeStatus.scraped_daily_dash_brooks).label("total_scraped_daily_dash_brooks"),
                func.sum(db.DateScrapeStatus.game_count_bbref).label("total_game_count_bbref"),
                func.sum(db.DateScrapeStatus.game_count_brooks).label("total_game_count_brooks"),
            ]
        )
        .select_from(
            join(
                db.Season,
                db.DateScrapeStatus,
                db.Season.id == db.DateScrapeStatus.season_id,
            )
        )
        .group_by(db.Season.id),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )
