"""Aggregate pitching stats for one team from a single game by combining data scraped from multiple sources."""
from app.main.util.s3_helper import get_bbref_boxscore_from_s3, get_all_pitchfx_logs_for_game_from_s3

def get_pitching_stats_for_game(session, bbref_game_id):
    result = get_bbref_boxscore_from_s3(bbref_game_id)
    if result.failure:
        return result
    boxscore = result.value
    result = get_all_pitchfx_logs_for_game_from_s3(session, bbref_game_id)
    if result.failure:
        return result
    pitchfx_logs = result.value
