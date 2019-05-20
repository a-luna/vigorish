"""Decode json dicts of scraped data to custom objects."""
from datetime import datetime, timezone
from dateutil import parser, tz

from app.main.scrape.bbref.models.bat_stats import BBRefBatStats
from app.main.scrape.bbref.models.bat_stats_detail import BBRefBatStatsDetail
from app.main.scrape.bbref.models.boxscore import BBRefBoxscore
from app.main.scrape.bbref.models.boxscore_game_meta import BBRefBoxscoreMeta
from app.main.scrape.bbref.models.boxscore_team_data import BBRefBoxscoreTeamData
from app.main.scrape.bbref.models.games_for_date import BBRefGamesForDate
from app.main.scrape.bbref.models.half_inning import BBRefHalfInning
from app.main.scrape.bbref.models.pbp_event import BBRefPlayByPlayEvent
from app.main.scrape.bbref.models.pbp_substitution import BBRefInGameSubstitution
from app.main.scrape.bbref.models.pitch_stats import BBRefPitchStats
from app.main.scrape.bbref.models.starting_lineup_slot import BBRefStartingLineupSlot
from app.main.scrape.bbref.models.umpire import BBRefUmpire
from app.main.scrape.brooks.models.games_for_date import BrooksGamesForDate
from app.main.scrape.brooks.models.game_info import BrooksGameInfo
from app.main.scrape.brooks.models.pitch_log import BrooksPitchLog
from app.main.scrape.brooks.models.pitch_logs_for_game import BrooksPitchLogsForGame
from app.main.util.result import Result


def decode_bbref_games_for_date(json_dict):
    """Convert json dictionary to BbrefGamesForDate"""
    if "__bbref_games_for_date__" in json_dict:
        try:
            games_for_date = BBRefGamesForDate()
            games_for_date.dashboard_url = json_dict["dashboard_url"]
            games_for_date.game_date_str = json_dict["game_date_str"]
            games_for_date.game_count = json_dict["game_count"]
            games_for_date.boxscore_urls = json_dict["boxscore_urls"]
            games_for_date.game_date = parser.parse(json_dict["game_date_str"])
            return Result.Ok(games_for_date)
        except Exception as e:
            error = f"Error: {repr(e)}"
            return Result.Fail(error)


def decode_brooks_game_info(json_dict):
    """Convert json dictionary to BrooksGameInfo object."""
    if "__brooks_game_info__" in json_dict:
        try:
            gameinfo = BrooksGameInfo()
            gameinfo.might_be_postponed = json_dict["might_be_postponed"]
            gameinfo.game_date_year = json_dict["game_date_year"]
            gameinfo.game_date_month = json_dict["game_date_month"]
            gameinfo.game_date_day = json_dict["game_date_day"]
            gameinfo.game_time_hour = json_dict["game_time_hour"]
            gameinfo.game_time_minute = json_dict["game_time_minute"]
            gameinfo.time_zone_name = json_dict["time_zone_name"]
            gameinfo.bb_game_id = json_dict["bb_game_id"]
            gameinfo.bbref_game_id = json_dict["bbref_game_id"]
            gameinfo.away_team_id_bb = json_dict["away_team_id_bb"]
            gameinfo.home_team_id_bb = json_dict["home_team_id_bb"]
            gameinfo.game_number_this_day = json_dict["game_number_this_day"]
            gameinfo.pitcher_appearance_count = json_dict["pitcher_appearance_count"]
            gameinfo.pitcher_appearance_dict = json_dict["pitcher_appearance_dict"]

            gameinfo.game_start_time = datetime(
                year=json_dict["game_date_year"],
                month=json_dict["game_date_month"],
                day=json_dict["game_date_day"],
                hour=json_dict["game_time_hour"],
                minute=json_dict["game_time_minute"],
                tzinfo=tz.gettz(json_dict["time_zone_name"]),
            )
            return Result.Ok(gameinfo)
        except Exception as e:
            error = f"Error: {repr(e)}"
            return Result.Fail(error)


def decode_brooks_games_for_date(json_dict):
    """Convert json dictionary to BrooksGamesForDate object."""
    if "__brooks_games_for_date__" in json_dict:
        try:
            games_for_date = BrooksGamesForDate()
            games_for_date.dashboard_url = json_dict["dashboard_url"]
            games_for_date.game_date_str = json_dict["game_date_str"]
            games_for_date.game_count = json_dict["game_count"]
            games_for_date.game_date = parser.parse(json_dict["game_date_str"])

            games_for_date.games = []
            for g in json_dict["games"]:
                result = decode_brooks_game_info(g)
                if result.failure:
                    return result
                games_for_date.games.append(result.value)

            return Result.Ok(games_for_date)
        except Exception as e:
            error = f"Error: {repr(e)}"
            return Result.Fail(error)


def decode_bbref_boxscore(json_dict):
    """Convert json dictionary to BBRefBoxscore object."""
    if "__bbref_boxscore__" in json_dict:
        try:
            boxscore = BBRefBoxscore()
            boxscore.boxscore_url = json_dict["boxscore_url"]
            boxscore.bbref_game_id = json_dict["bbref_game_id"]

            result = decode_bbref_boxscore_meta(json_dict["game_meta_info"])
            if result.failure:
                return result
            boxscore.game_meta_info = result.value

            result = decode_boxscore_team_data(json_dict["away_team_data"])
            if result.failure:
                return result
            boxscore.away_team_data = result.value

            result = decode_boxscore_team_data(json_dict["home_team_data"])
            if result.failure:
                return result
            boxscore.home_team_data = result.value

            boxscore.innings_list = []
            for i in json_dict["innings_list"]:
                result = decode_bbref_boxscore_half_inning(i)
                if result.failure:
                    return result
                boxscore.innings_list.append(result.value)

            boxscore.umpires = []
            for u in json_dict["umpires"]:
                result = decode_bbref_umpire(u)
                if result.failure:
                    return result
                boxscore.umpires.append(result.value)

            boxscore.player_team_dict = json_dict["player_team_dict"]
            boxscore.player_name_dict = json_dict["player_name_dict"]
            return Result.Ok(boxscore)
        except Exception as e:
            error = f"Error: {repr(e)}"
            return Result.Fail(error)


def decode_bbref_boxscore_meta(json_dict):
    if "__bbref_boxscore_meta__" in json_dict:
        try:
            boxscore_meta = BBRefBoxscoreMeta()
            boxscore_meta.park_name = json_dict["park_name"]
            boxscore_meta.field_type = json_dict["field_type"]
            boxscore_meta.day_night = json_dict["day_night"]
            boxscore_meta.first_pitch_temperature = json_dict["first_pitch_temperature"]
            boxscore_meta.first_pitch_precipitation = json_dict[
                "first_pitch_precipitation"
            ]
            boxscore_meta.first_pitch_wind = json_dict["first_pitch_wind"]
            boxscore_meta.first_pitch_clouds = json_dict["first_pitch_clouds"]
            boxscore_meta.game_duration = json_dict["game_duration"]
            boxscore_meta.attendance = json_dict["attendance"]
            return Result.Ok(boxscore_meta)
        except Exception as e:
            error = f"Error: {repr(e)}"
            return Result.Fail(error)


def decode_boxscore_team_data(json_dict):
    if "__bbref_boxscore_team_data__" in json_dict:
        try:
            team_data = BBRefBoxscoreTeamData(
                starting_lineup=[],
                batting_stats=[],
                pitching_stats=[]
            )
            team_data.team_id_br = json_dict["team_id_br"]
            team_data.total_wins_before_game = json_dict["total_wins_before_game"]
            team_data.total_losses_before_game = json_dict["total_losses_before_game"]
            team_data.total_runs_scored_by_team = json_dict["total_runs_scored_by_team"]
            team_data.total_runs_scored_by_opponent = json_dict["total_runs_scored_by_opponent"]
            team_data.total_hits_by_team = json_dict["total_hits_by_team"]
            team_data.total_hits_by_opponent = json_dict["total_hits_by_opponent"]
            team_data.total_errors_by_team = json_dict["total_errors_by_team"]
            team_data.total_errors_by_opponent = json_dict["total_errors_by_opponent"]

            team_data.starting_lineup = []
            for s in json_dict["starting_lineup"]:
                result = decode_bbref_lineup_slot(s)
                if result.failure:
                    return result
                team_data.starting_lineup.append(result.value)

            team_data.batting_stats = []
            for b in json_dict["batting_stats"]:
                result = decode_bbref_batting_stats(b)
                if result.failure:
                    return result
                team_data.batting_stats.append(result.value)

            team_data.pitching_stats = []
            for p in json_dict["pitching_stats"]:
                result = decode_bbref_pitching_stats(p)
                if result.failure:
                    return result
                team_data.pitching_stats.append(result.value)

            return Result.Ok(team_data)
        except Exception as e:
            error = f"Error: {repr(e)}"
            return Result.Fail(error)


def decode_bbref_boxscore_half_inning(json_dict):
    if "__bbref_boxscore_half_inning__" in json_dict:
        try:
            inning = BBRefHalfInning()
            inning.inning_id = json_dict["inning_id"]
            inning.inning_label = json_dict["inning_label"]
            inning.begin_inning_summary = json_dict["begin_inning_summary"]
            inning.end_inning_summary = json_dict["end_inning_summary"]
            inning.inning_total_runs = json_dict["inning_total_runs"]
            inning.inning_total_hits = json_dict["inning_total_hits"]
            inning.inning_total_errors = json_dict["inning_total_errors"]
            inning.inning_total_left_on_base = json_dict["inning_total_left_on_base"]
            inning.away_team_runs_after_inning = json_dict[
                "away_team_runs_after_inning"
            ]
            inning.home_team_runs_after_inning = json_dict[
                "home_team_runs_after_inning"
            ]

            inning.game_events = []
            for e in json_dict["game_events"]:
                result = decode_bbref_playbyplay_event(e)
                if result.failure:
                    return result
                inning.game_events.append(result.value)

            inning.substitutions = []
            for s in json_dict["substitutions"]:
                result = decode_bbref_pbp_substitution(s)
                if result.failure:
                    return result
                inning.substitutions.append(result.value)

            return Result.Ok(inning)
        except Exception as e:
            error = f"Error: {repr(e)}"
            return Result.Fail(error)


def decode_bbref_batting_stats(json_dict):
    try:
        bat_stats = BBRefBatStats(details=[])
        bat_stats.player_id_br = json_dict["player_id_br"]
        bat_stats.at_bats = int(json_dict["at_bats"])
        bat_stats.runs_scored = int(json_dict["runs_scored"])
        bat_stats.hits = int(json_dict["hits"])
        bat_stats.rbis = int(json_dict["rbis"])
        bat_stats.bases_on_balls = int(json_dict["bases_on_balls"])
        bat_stats.strikeouts = int(json_dict["strikeouts"])
        bat_stats.plate_appearances = int(json_dict["plate_appearances"])
        bat_stats.avg_to_date = float(json_dict["avg_to_date"])
        bat_stats.obp_to_date = float(json_dict["obp_to_date"])
        bat_stats.slg_to_date = float(json_dict["slg_to_date"])
        bat_stats.ops_to_date = float(json_dict["ops_to_date"])
        bat_stats.total_pitches = int(json_dict["total_pitches"])
        bat_stats.total_strikes = int(json_dict["total_strikes"])
        bat_stats.wpa_bat = float(json_dict["wpa_bat"])
        bat_stats.avg_lvg_index = float(json_dict["avg_lvg_index"])
        bat_stats.wpa_bat_pos = float(json_dict["wpa_bat_pos"])
        bat_stats.wpa_bat_neg = float(json_dict["wpa_bat_neg"])
        bat_stats.re24_bat = float(json_dict["re24_bat"])

        bat_stats.details = []
        for d in json_dict["details"]:
            result = decode_bbref_bat_stats_detail(d)
            if result.failure:
                return result
            bat_stats.details.append(result.value)

        return Result.Ok(bat_stats)
    except Exception as e:
        error = f"Error: {repr(e)}"
        return Result.Fail(error)


def decode_bbref_bat_stats_detail(json_dict):
    try:
        detail = BBRefBatStatsDetail()
        detail.count = int(json_dict["count"])
        detail.stat = json_dict["stat"]
        return Result.Ok(detail)
    except Exception as e:
        error = f"Error: {repr(e)}"
        return Result.Fail(error)


def decode_bbref_pitching_stats(json_dict):
    try:
        pitch_stats = BBRefPitchStats()
        pitch_stats.player_id_br = json_dict["player_id_br"]
        pitch_stats.innings_pitched = float(json_dict["innings_pitched"])
        pitch_stats.hits = int(json_dict["hits"])
        pitch_stats.runs = int(json_dict["runs"])
        pitch_stats.earned_runs = int(json_dict["earned_runs"])
        pitch_stats.bases_on_balls = int(json_dict["bases_on_balls"])
        pitch_stats.strikeouts = int(json_dict["strikeouts"])
        pitch_stats.homeruns = int(json_dict["homeruns"])
        pitch_stats.batters_faced = int(json_dict["batters_faced"])
        pitch_stats.pitch_count = int(json_dict["pitch_count"])
        pitch_stats.strikes = int(json_dict["strikes"])
        pitch_stats.strikes_contact = int(json_dict["strikes_contact"])
        pitch_stats.strikes_swinging = int(json_dict["strikes_swinging"])
        pitch_stats.strikes_looking = int(json_dict["strikes_looking"])
        pitch_stats.ground_balls = int(json_dict["ground_balls"])
        pitch_stats.fly_balls = int(json_dict["fly_balls"])
        pitch_stats.line_drives = int(json_dict["line_drives"])
        pitch_stats.unknown_type = int(json_dict["unknown_type"])
        pitch_stats.game_score = int(json_dict["game_score"])
        pitch_stats.inherited_runners = int(json_dict["inherited_runners"])
        pitch_stats.inherited_scored = int(json_dict["inherited_scored"])
        pitch_stats.wpa_pitch = float(json_dict["wpa_pitch"])
        pitch_stats.avg_lvg_index = float(json_dict["avg_lvg_index"])
        pitch_stats.re24_pitch = float(json_dict["re24_pitch"])
        return Result.Ok(pitch_stats)
    except Exception as e:
        error = f"Error: {repr(e)}"
        return Result.Fail(error)


def decode_bbref_umpire(json_dict):
    try:
        umpire = BBRefUmpire()
        umpire.field_location = json_dict["field_location"]
        umpire.umpire_name = json_dict["umpire_name"]
        return Result.Ok(umpire)
    except Exception as e:
        error = f"Error: {repr(e)}"
        return Result.Fail(error)


def decode_bbref_lineup_slot(json_dict):
    try:
        lineup_slot = BBRefStartingLineupSlot()
        lineup_slot.player_id_br = json_dict["player_id_br"]
        lineup_slot.bat_order = int(json_dict["bat_order"])
        lineup_slot.def_position = json_dict["def_position"]
        return Result.Ok(lineup_slot)
    except Exception as e:
        error = f"Error: {repr(e)}"
        return Result.Fail(error)


def decode_bbref_playbyplay_event(json_dict):
    if "__bbref_pbp_game_event__" in json_dict:
        try:
            pbp_event = BBRefPlayByPlayEvent()
            pbp_event.pbp_table_row_number = json_dict["pbp_table_row_number"]
            pbp_event.inning_id = json_dict["inning_id"]
            pbp_event.inning_label = json_dict["inning_label"]
            pbp_event.score = json_dict["score"]
            pbp_event.outs_before_play = int(json_dict["outs_before_play"])
            pbp_event.runners_on_base = json_dict["runners_on_base"]
            pbp_event.pitch_sequence = json_dict["pitch_sequence"]
            pbp_event.runs_outs_result = json_dict["runs_outs_result"]
            pbp_event.team_batting_id_br = json_dict["team_batting_id_br"]
            pbp_event.team_pitching_id_br = json_dict["team_pitching_id_br"]
            pbp_event.play_description = json_dict["play_description"]
            pbp_event.pitcher_id_br = json_dict["pitcher_id_br"]
            pbp_event.batter_id_br = json_dict["batter_id_br"]
            return Result.Ok(pbp_event)
        except Exception as e:
            error = f"Error: {repr(e)}"
            return Result.Fail(error)


def decode_bbref_pbp_substitution(json_dict):
    if "__bbref_pbp_in_game_substitution__" in json_dict:
        try:
            substitution = BBRefInGameSubstitution()
            substitution.inning_id = json_dict["inning_id"]
            substitution.inning_label = json_dict["inning_label"]
            substitution.pbp_table_row_number = json_dict["pbp_table_row_number"]
            substitution.sub_description = json_dict["sub_description"]
            substitution.incoming_player_id_br = json_dict["incoming_player_id_br"]
            substitution.incoming_player_pos = json_dict["incoming_player_pos"]
            substitution.outgoing_player_id_br = json_dict["outgoing_player_id_br"]
            substitution.outgoing_player_pos = json_dict["outgoing_player_pos"]
            substitution.lineup_slot = json_dict["lineup_slot"]
            return Result.Ok(substitution)
        except Exception as e:
            error = f"Error: {repr(e)}"
            return Result.Fail(error)


def decode_brooks_pitch_logs_for_game(json_dict):
    if "__brooks_pitch_logs_for_game__" in json_dict:
        try:
            pitch_logs_for_game = BrooksPitchLogsForGame()
            pitch_logs_for_game.bb_game_id = json_dict["bb_game_id"]
            pitch_logs_for_game.bbref_game_id = json_dict["bbref_game_id"]
            pitch_logs_for_game.pitch_log_count = json_dict["pitch_log_count"]

            pitch_logs_for_game.pitch_logs = []
            for log in json_dict["pitch_logs"]:
                result = decode_brooks_pitch_log(log)
                if result.failure:
                    return result
                pitch_logs_for_game.pitch_logs.append(result.value)

            return Result.Ok(pitch_logs_for_game)
        except Exception as e:
            error = f"Error: {repr(e)}"
            return Result.Fail(error)


def decode_brooks_pitch_log(json_dict):
    if "__brooks_pitch_log__" in json_dict:
        try:
            pitch_log = BrooksPitchLog()
            pitch_log.parsed_all_info = json_dict["parsed_all_info"]
            pitch_log.pitcher_name = json_dict["pitcher_name"]
            pitch_log.pitcher_id_mlb = json_dict["pitcher_id_mlb"]
            pitch_log.pitch_app_id = json_dict["pitch_app_id"]
            pitch_log.total_pitch_count = json_dict["total_pitch_count"]
            pitch_log.pitch_count_by_inning = json_dict["total_pitch_count"]
            pitch_log.pitcher_team_id_bb = json_dict["pitcher_team_id_bb"]
            pitch_log.opponent_team_id_bb = json_dict["opponent_team_id_bb"]
            pitch_log.bb_game_id = json_dict["bb_game_id"]
            pitch_log.bbref_game_id = json_dict["bbref_game_id"]
            pitch_log.pitchfx_url = json_dict["pitchfx_url"]
            pitch_log.pitch_log_url = json_dict["pitch_log_url"]
            return Result.Ok(pitch_log)
        except Exception as e:
            error = f"Error: {repr(e)}"
            return Result.Fail(error)
