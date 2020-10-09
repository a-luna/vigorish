from sqlalchemy import func, join, select
from sqlalchemy_utils import create_view

from vigorish.config.database import Base, GameScrapeStatus, Season


class Season_Game_View(Base):
    __table__ = create_view(
        name="season_game_status",
        selectable=select(
            [
                Season.id.label("id"),
                func.count(GameScrapeStatus.id).label("total_games"),
                func.sum(GameScrapeStatus.scraped_bbref_boxscore).label(
                    "total_scraped_bbref_boxscore"
                ),
                func.sum(GameScrapeStatus.scraped_brooks_pitch_logs).label(
                    "total_scraped_brooks_pitch_logs"
                ),
                func.sum(GameScrapeStatus.combined_data_success).label(
                    "total_combined_data_success"
                ),
                func.sum(GameScrapeStatus.combined_data_fail).label("total_combined_data_fail"),
                func.sum(GameScrapeStatus.pitch_app_count_bbref).label(
                    "total_pitch_app_count_bbref"
                ),
                func.sum(GameScrapeStatus.pitch_app_count_brooks).label(
                    "total_pitch_app_count_brooks"
                ),
                func.sum(GameScrapeStatus.total_pitch_count_bbref).label("total_pitch_count_bbref"),
            ]
        )
        .select_from(
            join(
                Season,
                GameScrapeStatus,
                Season.id == GameScrapeStatus.season_id,
                isouter=True,
            )
        )
        .group_by(Season.id),
        metadata=Base.metadata,
        cascade_on_drop=False,
    )
