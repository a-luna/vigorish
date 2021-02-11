from sqlalchemy import func, join, select
from sqlalchemy_utils import create_view

import vigorish.database as db


class Season_Game_View(db.Base):
    __table__ = create_view(
        name="season_game_status",
        selectable=select(
            [
                db.Season.id.label("id"),
                func.count(db.GameScrapeStatus.id).label("total_games"),
                func.sum(db.GameScrapeStatus.scraped_bbref_boxscore).label("total_scraped_bbref_boxscore"),
                func.sum(db.GameScrapeStatus.scraped_brooks_pitch_logs).label("total_scraped_brooks_pitch_logs"),
                func.sum(db.GameScrapeStatus.combined_data_success).label("total_combined_data_success"),
                func.sum(db.GameScrapeStatus.combined_data_fail).label("total_combined_data_fail"),
                func.sum(db.GameScrapeStatus.pitch_app_count_bbref).label("total_pitch_app_count_bbref"),
                func.sum(db.GameScrapeStatus.pitch_app_count_brooks).label("total_pitch_app_count_brooks"),
                func.sum(db.GameScrapeStatus.total_pitch_count_bbref).label("total_pitch_count_bbref"),
            ]
        )
        .select_from(
            join(
                db.Season,
                db.GameScrapeStatus,
                db.Season.id == db.GameScrapeStatus.season_id,
            )
        )
        .group_by(db.Season.id),
        metadata=db.Base.metadata,
        cascade_on_drop=False,
    )
