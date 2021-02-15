"""Decode json dicts of scraped data to custom objects."""
from dacite import from_dict
from dateutil import parser

from vigorish.patch.bbref_boxscores import BBRefBoxscorePatchList, PatchBBRefBoxscorePitchSequence
from vigorish.patch.bbref_games_for_date import (
    BBRefGamesForDatePatchList,
    PatchBBRefGamesForDateGameID,
)
from vigorish.patch.brooks_games_for_date import (
    BrooksGamesForDatePatchList,
    PatchBrooksGamesForDateBBRefGameID,
    PatchBrooksGamesForDateRemoveGame,
)
from vigorish.patch.brooks_pitchfx import (
    BrooksPitchFxPatchList,
    PatchBrooksPitchFxBatterId,
    PatchBrooksPitchFxDeletePitchFx,
    PatchBrooksPitchFxPitcherId,
)
from vigorish.scrape.bbref_boxscores.models.bat_stats import BBRefBatStats
from vigorish.scrape.bbref_boxscores.models.bat_stats_detail import BBRefBatStatsDetail
from vigorish.scrape.bbref_boxscores.models.boxscore import BBRefBoxscore
from vigorish.scrape.bbref_boxscores.models.boxscore_game_meta import BBRefBoxscoreMeta
from vigorish.scrape.bbref_boxscores.models.boxscore_team_data import BBRefBoxscoreTeamData
from vigorish.scrape.bbref_boxscores.models.half_inning import BBRefHalfInning
from vigorish.scrape.bbref_boxscores.models.pbp_event import BBRefPlayByPlayEvent
from vigorish.scrape.bbref_boxscores.models.pbp_other import BBRefPlayByPlayMiscEvent
from vigorish.scrape.bbref_boxscores.models.pbp_substitution import BBRefInGameSubstitution
from vigorish.scrape.bbref_boxscores.models.pitch_stats import BBRefPitchStats
from vigorish.scrape.bbref_boxscores.models.starting_lineup_slot import BBRefStartingLineupSlot
from vigorish.scrape.bbref_boxscores.models.umpire import BBRefUmpire
from vigorish.scrape.bbref_games_for_date.models.game_info import BBRefGameInfo
from vigorish.scrape.bbref_games_for_date.models.games_for_date import BBRefGamesForDate
from vigorish.scrape.brooks_games_for_date.models.game_info import BrooksGameInfo
from vigorish.scrape.brooks_games_for_date.models.games_for_date import BrooksGamesForDate
from vigorish.scrape.brooks_pitch_logs.models.pitch_log import BrooksPitchLog
from vigorish.scrape.brooks_pitch_logs.models.pitch_logs_for_game import BrooksPitchLogsForGame
from vigorish.scrape.brooks_pitchfx.models.pitchfx import BrooksPitchFxData
from vigorish.scrape.brooks_pitchfx.models.pitchfx_log import BrooksPitchFxLog
from vigorish.util.result import Result


def decode_bbref_games_for_date(json_dict):
    """Convert json dictionary to BbrefGamesForDate object."""
    json_dict.pop("__bbref_games_for_date__")
    json_dict["game_date"] = parser.parse(json_dict["game_date_str"])
    games_dict_list = json_dict.pop("games")
    games_for_date = BBRefGamesForDate(**json_dict)
    games_for_date.games = [decode_bbref_game_info(g) for g in games_dict_list]
    return Result.Ok(games_for_date)


def decode_bbref_game_info(json_dict):
    """Convert json dictionary to BBrefGameInfo object."""
    json_dict.pop("__bbref_game_info__")
    return BBRefGameInfo(**json_dict)


def decode_brooks_games_for_date(json_dict):
    """Convert json dictionary to BrooksGamesForDate object."""
    json_dict.pop("__brooks_games_for_date__")
    json_dict["game_date"] = parser.parse(json_dict["game_date_str"])
    games_dict_list = json_dict.pop("games")
    games_for_date = BrooksGamesForDate(**json_dict)
    games_for_date.games = [decode_brooks_game_info(g) for g in games_dict_list]
    return Result.Ok(games_for_date)


def decode_brooks_game_info(json_dict):
    """Convert json dictionary to BrooksGameInfo object."""
    json_dict.pop("__brooks_game_info__")
    return BrooksGameInfo(**json_dict)


def decode_bbref_boxscore(json_dict):
    """Convert json dictionary to BBRefBoxscore object."""
    json_dict.pop("__bbref_boxscore__")
    game_meta_info_dict = json_dict.pop("game_meta_info")
    away_team_dict = json_dict.pop("away_team_data")
    home_team_dict = json_dict.pop("home_team_data")
    innings_dict_list = json_dict.pop("innings_list")
    umpire_dict_list = json_dict.pop("umpires")
    boxscore = BBRefBoxscore(**json_dict)
    boxscore.game_meta_info = decode_bbref_boxscore_meta(game_meta_info_dict)
    boxscore.away_team_data = decode_boxscore_team_data(away_team_dict)
    boxscore.home_team_data = decode_boxscore_team_data(home_team_dict)
    boxscore.innings_list = [decode_bbref_boxscore_half_inning(i) for i in innings_dict_list]
    boxscore.umpires = [BBRefUmpire(**u) for u in umpire_dict_list]
    return Result.Ok(boxscore)


def decode_bbref_boxscore_meta(json_dict):
    json_dict.pop("__bbref_boxscore_meta__")
    return BBRefBoxscoreMeta(**json_dict)


def decode_boxscore_team_data(json_dict):
    json_dict.pop("__bbref_boxscore_team_data__")
    lineup_dict_list = json_dict.pop("starting_lineup")
    bat_stats_dict_list = json_dict.pop("batting_stats")
    pitch_stats_dict_list = json_dict.pop("pitching_stats")
    team_won = json_dict.pop("team_won")
    team_data = BBRefBoxscoreTeamData(**json_dict)
    team_data.team_won = team_won
    team_data.starting_lineup = [BBRefStartingLineupSlot(**lu) for lu in lineup_dict_list]
    team_data.batting_stats = [decode_bbref_bat_stats(bs) for bs in bat_stats_dict_list]
    team_data.pitching_stats = [BBRefPitchStats(**ps) for ps in pitch_stats_dict_list]
    return team_data


def decode_bbref_bat_stats(json_dict):
    details_dict_list = json_dict.pop("details")
    bat_stats = BBRefBatStats(**json_dict)
    bat_stats.details = [BBRefBatStatsDetail(**d) for d in details_dict_list]
    return bat_stats


def decode_bbref_boxscore_half_inning(json_dict):
    json_dict.pop("__bbref_boxscore_half_inning__")
    game_event_dict_list = json_dict.pop("game_events")
    player_sub_dict_list = json_dict.pop("substitutions")
    misc_event_dict_list = json_dict.pop("misc_events")
    inning = BBRefHalfInning(**json_dict)
    inning.game_events = [decode_bbref_playbyplay_event(e) for e in game_event_dict_list]
    inning.substitutions = [decode_bbref_pbp_substitution(s) for s in player_sub_dict_list]
    inning.misc_events = [decode_bbref_pbp_misc_event(m) for m in misc_event_dict_list]
    return inning


def decode_bbref_playbyplay_event(json_dict):
    json_dict.pop("__bbref_pbp_game_event__")
    return BBRefPlayByPlayEvent(**json_dict)


def decode_bbref_pbp_substitution(json_dict):
    json_dict.pop("__bbref_pbp_in_game_substitution__")
    return BBRefInGameSubstitution(**json_dict)


def decode_bbref_pbp_misc_event(json_dict):
    json_dict.pop("__bbref_pbp_misc_event__")
    return BBRefPlayByPlayMiscEvent(**json_dict)


def decode_brooks_pitch_logs_for_game(json_dict):
    json_dict.pop("__brooks_pitch_logs_for_game__")
    pitch_log_dict_list = json_dict.pop("pitch_logs")
    pitch_logs_for_game = BrooksPitchLogsForGame(**json_dict)
    pitch_logs_for_game.pitch_logs = [decode_brooks_pitch_log(pl) for pl in pitch_log_dict_list]
    return Result.Ok(pitch_logs_for_game)


def decode_brooks_pitch_log(json_dict):
    json_dict.pop("__brooks_pitch_log__")
    return BrooksPitchLog(**json_dict)


def decode_brooks_pitchfx_log(json_dict):
    json_dict.pop("__brooks_pitchfx_log__")
    pfx_dict_list = json_dict.pop("pitchfx_log")
    pitchfx_log = BrooksPitchFxLog(**json_dict)
    pitchfx_log.pitchfx_log = [decode_brooks_pitchfx_data(pfx) for pfx in pfx_dict_list]
    return Result.Ok(pitchfx_log)


def decode_brooks_pitchfx_data(json_dict):
    json_dict.pop("__brooks_pitchfx_data__")
    return from_dict(data_class=BrooksPitchFxData, data=json_dict)


def decode_bbref_games_for_date_patch_list(json_dict):
    json_dict.pop("__bbref_games_for_date_patch_list__")
    patch_dict_list = json_dict.pop("patch_list")
    bbref_games_for_date_patch_list = BBRefGamesForDatePatchList(**json_dict)
    bbref_games_for_date_patch_list.patch_list = [decode_patch_bbref_games_for_date_game_id(p) for p in patch_dict_list]
    return Result.Ok(bbref_games_for_date_patch_list)


def decode_patch_bbref_games_for_date_game_id(json_dict):
    json_dict.pop("__patch_bbref_games_for_date_game_id__")
    return from_dict(data_class=PatchBBRefGamesForDateGameID, data=json_dict)


def decode_bbref_boxscore_patch_list(json_dict):
    json_dict.pop("__bbref_boxscore_patch_list__")
    patch_dict_list = json_dict.pop("patch_list")
    boxscore_patch_list = BBRefBoxscorePatchList(**json_dict)
    boxscore_patch_list.patch_list = [decode_patch_bbref_boxscore_pitch_sequence(p) for p in patch_dict_list]
    return Result.Ok(boxscore_patch_list)


def decode_patch_bbref_boxscore_pitch_sequence(json_dict):
    json_dict.pop("__patch_bbref_boxscore_pitch_sequence__")
    return from_dict(data_class=PatchBBRefBoxscorePitchSequence, data=json_dict)


def decode_brooks_games_for_date_patch_list(json_dict):
    json_dict.pop("__brooks_games_for_date_patch_list__")
    patch_dict_list = json_dict.pop("patch_list")
    brooks_games_for_date_patch_list = BrooksGamesForDatePatchList(**json_dict)
    for patch_json in patch_dict_list:
        if "__patch_brooks_games_for_date_bbref_game_id__" in patch_json:
            patch = decode_patch_brooks_games_for_date_bbref_game_id(patch_json)
        elif "__patch_brooks_games_for_date_remove_game__" in patch_json:
            patch = decode_patch_brooks_games_for_date_remove_game(patch_json)
        else:
            return Result.Fail("JSON file is not in the expected format.")
        brooks_games_for_date_patch_list.patch_list.append(patch)
    return Result.Ok(brooks_games_for_date_patch_list)


def decode_patch_brooks_games_for_date_bbref_game_id(json_dict):
    json_dict.pop("__patch_brooks_games_for_date_bbref_game_id__")
    return from_dict(data_class=PatchBrooksGamesForDateBBRefGameID, data=json_dict)


def decode_patch_brooks_games_for_date_remove_game(json_dict):
    json_dict.pop("__patch_brooks_games_for_date_remove_game__")
    return from_dict(data_class=PatchBrooksGamesForDateRemoveGame, data=json_dict)


def decode_brooks_pitchfx_patch_list(json_dict):
    json_dict.pop("__brooks_pitchfx_patch_list__")
    patch_dict_list = json_dict.pop("patch_list")
    pitchfx_patch_list = BrooksPitchFxPatchList(**json_dict)
    for patch_json in patch_dict_list:
        if "__patch_brooks_pitchfx_batter_id__" in patch_json:
            patch = decode_patch_brooks_pitchfx_batter_id(patch_json)
        elif "__patch_brooks_pitchfx_pitcher_id__" in patch_json:
            patch = decode_patch_brooks_pitchfx_pitcher_id(patch_json)
        elif "__patch_brooks_pitchfx_delete_pitchfx__" in patch_json:
            patch = decode_patch_brooks_pitchfx_delete_pitchfx(patch_json)
        else:
            return Result.Fail("JSON file is not in the expected format.")
        pitchfx_patch_list.patch_list.append(patch)
    return Result.Ok(pitchfx_patch_list)


def decode_patch_brooks_pitchfx_batter_id(json_dict):
    json_dict.pop("__patch_brooks_pitchfx_batter_id__")
    return from_dict(data_class=PatchBrooksPitchFxBatterId, data=json_dict)


def decode_patch_brooks_pitchfx_delete_pitchfx(json_dict):
    json_dict.pop("__patch_brooks_pitchfx_delete_pitchfx__")
    return from_dict(data_class=PatchBrooksPitchFxDeletePitchFx, data=json_dict)


def decode_patch_brooks_pitchfx_pitcher_id(json_dict):
    json_dict.pop("__patch_brooks_pitchfx_pitcher_id__")
    return from_dict(data_class=PatchBrooksPitchFxPitcherId, data=json_dict)
