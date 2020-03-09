"""Reset/delete all data for a single date."""
from app.main.models.status_date import DateScrapeStatus
from app.main.models.status_game import GameScrapeStatus
from app.main.models.status_pitch_appearance import PitchAppearanceScrapeStatus
from app.main.util.dt_format_strings import DATE_ONLY_TABLE_ID
from app.main.util.s3_helper import (
    delete_bbref_boxscore_from_s3,
    delete_bbref_games_for_date_from_s3,
    delete_brooks_games_for_date_from_s3,
    delete_brooks_pitch_logs_for_game_from_s3,
    delete_brooks_pitchfx_log_from_s3,
)
from app.main.util.result import Result


def reset_date(session, date):
    pitch_app_ids_for_date = PitchAppearanceScrapeStatus.get_all_pitch_app_ids_for_date(
        session, date
    )
    bb_game_ids_for_date = GameScrapeStatus.get_all_brooks_game_ids_for_date(session, date)
    bbref_game_ids_for_date = GameScrapeStatus.get_all_bbref_game_ids_for_date(session, date)
    for pitch_app_id in pitch_app_ids_for_date:
        delete_brooks_pitchfx_log_from_s3(pitch_app_id)
    for brooks_game_id in bb_game_ids_for_date:
        delete_brooks_pitch_logs_for_game_from_s3(brooks_game_id)
    for bbref_game_id in bbref_game_ids_for_date:
        delete_bbref_boxscore_from_s3(bbref_game_id)
    delete_brooks_games_for_date_from_s3(date)
    delete_bbref_games_for_date_from_s3(date)

    try:
        date_id = date.strftime(DATE_ONLY_TABLE_ID)
        session.query(PitchAppearanceScrapeStatus).filter_by(
            scrape_status_date_id=int(date_id)
        ).delete()
        session.query(GameScrapeStatus).filter_by(scrape_status_date_id=int(date_id)).delete()
        date_status = DateScrapeStatus.find_by_date(session, date)
        setattr(date_status, "scraped_daily_dash_bbref", 0)
        setattr(date_status, "game_count_bbref", 0)
        setattr(date_status, "scraped_daily_dash_brooks", 0)
        setattr(date_status, "game_count_brooks", 0)
        session.commit()
        return Result.Ok()
    except Exception as e:
        session.rollback()
        return Result.Fail(f"Error: {repr(e)}")
