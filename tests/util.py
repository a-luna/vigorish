from vigorish.config.database import DateScrapeStatus, GameScrapeStatus, PitchAppScrapeStatus
from vigorish.util.dt_format_strings import DATE_ONLY_TABLE_ID


def reset_date_scrape_status_after_parsed_bbref_games_for_date(db_session, game_date):
    date_status = db_session.query(DateScrapeStatus).get(game_date.strftime(DATE_ONLY_TABLE_ID))
    setattr(date_status, "scraped_daily_dash_bbref", 0)
    setattr(date_status, "game_count_bbref", 0)
    db_session.commit()


def reset_date_scrape_status_after_parsed_brooks_games_for_date(db_session, game_date):
    date_status = db_session.query(DateScrapeStatus).get(game_date.strftime(DATE_ONLY_TABLE_ID))
    setattr(date_status, "scraped_daily_dash_brooks", 0)
    setattr(date_status, "game_count_brooks", 0)
    db_session.commit()


def reset_game_scrape_status_after_parsed_brooks_games_for_date(db_session, brooks_games_for_date):
    for game_info in brooks_games_for_date.games:
        if not game_info.pitcher_appearance_count:
            continue
        game_status = GameScrapeStatus.find_by_bbref_game_id(db_session, game_info.bbref_game_id)
        setattr(game_status, "game_time_hour", None)
        setattr(game_status, "game_time_minute", None)
        setattr(game_status, "game_time_zone", None)
        setattr(game_status, "pitch_app_count_brooks", 0)
        db_session.commit()


def reset_game_scrape_status_after_parsed_boxscore(db_session, bbref_game_id):
    game_status = GameScrapeStatus.find_by_bbref_game_id(db_session, bbref_game_id)
    setattr(game_status, "scraped_bbref_boxscore", 0)
    setattr(game_status, "pitch_app_count_bbref", 0)
    setattr(game_status, "total_pitch_count_bbref", 0)
    db_session.commit()


def reset_game_scrape_status_after_parsed_pitch_logs(db_session, bb_game_id):
    game_status = GameScrapeStatus.find_by_bb_game_id(db_session, bb_game_id)
    setattr(game_status, "scraped_brooks_pitch_logs", 0)
    setattr(game_status, "total_pitch_count_brooks", 0)
    db_session.commit()


def reset_pitch_app_scrape_status_after_parsed_pitch_logs(db_session, pitch_logs_for_game):
    for pitch_log in pitch_logs_for_game.pitch_logs:
        pitch_app_id = pitch_log.pitch_app_id
        pitch_app_status = PitchAppScrapeStatus.find_by_pitch_app_id(db_session, pitch_app_id)
        db_session.delete(pitch_app_status)
        db_session.commit()


def reset_pitch_app_scrape_status_after_parsed_pitchfx(db_session, pitch_app_id):
    pitch_app_status = PitchAppScrapeStatus.find_by_pitch_app_id(db_session, pitch_app_id)
    setattr(pitch_app_status, "scraped_pitchfx", 0)
    setattr(pitch_app_status, "pitch_count_pitchfx", 0)
    db_session.commit()


def reset_pitch_app_scrape_status_after_combined_data(db_session, bbref_game_id):
    pa_ids = PitchAppScrapeStatus.get_all_scraped_pitch_app_ids_for_game_with_pitchfx_data(
        db_session, bbref_game_id
    )
    for pitch_app_id in pa_ids:
        pitch_app_status = PitchAppScrapeStatus.find_by_pitch_app_id(db_session, pitch_app_id)
        setattr(pitch_app_status, "audit_successful", 0)
        setattr(pitch_app_status, "audit_failed", 0)
        setattr(pitch_app_status, "pitchfx_data_complete", 0)
        setattr(pitch_app_status, "pitch_count_bbref", 0)
        setattr(pitch_app_status, "pitch_count_pitchfx_audited", 0)
        setattr(pitch_app_status, "duplicate_pitchfx_removed_count", 0)
        setattr(pitch_app_status, "pitch_count_missing_pitchfx", 0)
        setattr(pitch_app_status, "missing_pitchfx_is_valid", 0)
        setattr(pitch_app_status, "batters_faced_bbref", 0)
        setattr(pitch_app_status, "total_at_bats_pitchfx_complete", 0)
        setattr(pitch_app_status, "total_at_bats_missing_pitchfx", 0)
        db_session.commit()
