import re
from string import Template

from lxml import html
from w3lib.url import url_query_parameter

from vigorish.constants import DEFENSE_POSITIONS, VENUE_TERMS
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
from vigorish.util.numeric_helpers import is_even
from vigorish.util.result import Result
from vigorish.util.string_helpers import fuzzy_match

_TEAM_ID_XPATH = '//a[@itemprop="name"]/@href'
_AWAY_TEAM_RECORD_XPATH = '//div[@class="scorebox"]/div[1]/div[3]/text()'
_HOME_TEAM_RECORD_XPATH = '//div[@class="scorebox"]/div[2]/div[3]/text()'
_SCOREBOX_META_XPATH = '//div[@class="scorebox_meta"]//div/text()'
_FIRST_PITCH_WEATHER_XPATH = '//strong[contains(text(), "Start Time Weather")]/../text()'
_LINESCORE_KEYS_XPATH = '//table[contains(@class, "linescore")]//th/text()'
_LINESCORE_AWAY_VALS_XPATH = '//table[contains(@class, "linescore")]//tbody/tr[1]//td/text()'
_LINESCORE_HOME_VALS_XPATH = '//table[contains(@class, "linescore")]//tbody/tr[2]//td/text()'
_LINESCORE_W_L_SV_XPATH = '//table[contains(@class, "linescore")]//tfoot/tr/td/text()'
_UMPIRES_XPATH = '//strong[contains(text(), "Umpires")]/../text()'

_BAT_STATS_TABLE_XPATH = '//table[contains(@id, "batting")][not(contains(@id, "clone"))]'
_BATTER_IDS_XPATH = './tbody//td[@data-stat="batting_avg"]/../th[@data-stat="player"]/@data-append-csv'
_BATTER_NAMES_XPATH = './tbody//td[@data-stat="batting_avg"]/../th[@data-stat="player"]//a/text()'
_T_BAT_STATS_ROW_XPATH = './tbody//th[@data-append-csv="${pid}"]/..'
_T_BAT_STATS_XPATH = './td[@data-stat="${stat}"]/text()'

_PITCH_STATS_TABLE_XPATH = '//table[contains(@id, "pitching")][not(contains(@id, "clone"))]'
_PITCHER_IDS_XPATH = './tbody//td[@data-stat="earned_run_avg"]/../th[@data-stat="player"]/@data-append-csv'
_PITCHER_NAMES_XPATH = './tbody//td[@data-stat="earned_run_avg"]/../th[@data-stat="player"]//a/text()'
_T_PITCH_STATS_ROW_XPATH = './tbody//th[@data-append-csv="${pid}"]/..'
_T_PITCH_STAT_XPATH = './td[@data-stat="${stat}"]/text()'

_AWAY_LINEUP_ORDER_XPATH = '//div[@id="lineups_1"]//table//tbody//tr//td[1]/text()'
_AWAY_LINEUP_PLAYER_XPATH = '//div[@id="lineups_1"]//table//tbody//a/@href'
_AWAY_LINEUP_DEF_POS_XPATH = '//div[@id="lineups_1"]//table//tbody//tr//td[3]/text()'
_HOME_LINEUP_ORDER_XPATH = '//div[@id="lineups_2"]//table//tbody//tr//td[1]/text()'
_HOME_LINEUP_PLAYER_XPATH = '//div[@id="lineups_2"]//table//tbody//a/@href'
_HOME_LINEUP_DEF_POS_XPATH = '//div[@id="lineups_2"]//table//tbody//tr//td[3]/text()'

_PBP_TABLE_XPATH = '//table[contains(@id, "play_by_play")][not(contains(@id, "clone"))]'
_PBP_INN_SUM_TOP_XPATH = './tbody//th[@data-stat="inning_summary_12"]/text()'
_PBP_INN_SUM_TOP_ROW_NUM_XPATH = './tbody//tr[@class="pbp_summary_top"]/@data-row'
_PBP_INN_SUM_BOT_XPATH = './tbody//tr[@class="pbp_summary_bottom"]//td[last()]/text()'
_PBP_INN_SUM_BOT_ROW_NUM_XPATH = './tbody//tr[@class="pbp_summary_bottom"]/@data-row'
_PBP_INN_SUM_BOT_LAST_XPATH = './tbody//tr[@class="pbp_summary_bottom"]//span[@class="half_inning_summary"]/text()'
_PBP_PLAYER_SUB_ROW_NUM_XPATH = './tbody//tr[@class="ingame_substitution"]/@data-row'
_T_PBP_PLAYER_SUB_XPATH = (
    './tbody//tr[@class="ingame_substitution"][@data-row="${row}"]' '//td[@data-stat="inning_summary_3"]//div/text()'
)
_T_PBP_MISC_EVENT_XPATH = './tbody//tr[@data-row="${row}"]//td[@data-stat="inning_summary_3"]//text()'
_PBP_INNING_XPATH = './tbody//tr[not(contains(@class, "thead"))]/th[@data-stat="inning"]/text()'

BAT_STATS = {
    "at_bats": "AB",
    "runs_scored": "R",
    "hits": "H",
    "rbis": "RBI",
    "bases_on_balls": "BB",
    "strikeouts": "SO",
    "plate_appearances": "PA",
    "avg_to_date": "batting_avg",
    "obp_to_date": "onbase_perc",
    "slg_to_date": "slugging_perc",
    "ops_to_date": "onbase_plus_slugging",
    "total_pitches": "pitches",
    "total_strikes": "strikes_total",
    "wpa_bat": "wpa_bat",
    "avg_lvg_index": "leverage_index_avg",
    "wpa_bat_pos": "wpa_bat_pos",
    "wpa_bat_neg": "wpa_bat_neg",
    "re24_bat": "re24_bat",
    "details": "details",
}

PITCH_STATS = {
    "innings_pitched": "IP",
    "hits": "H",
    "runs": "R",
    "earned_runs": "ER",
    "bases_on_balls": "BB",
    "strikeouts": "SO",
    "homeruns": "HR",
    "batters_faced": "batters_faced",
    "pitch_count": "pitches",
    "strikes": "strikes_total",
    "strikes_contact": "strikes_contact",
    "strikes_swinging": "strikes_swinging",
    "strikes_looking": "strikes_looking",
    "ground_balls": "inplay_gb_total",
    "fly_balls": "inplay_fb_total",
    "line_drives": "inplay_ld",
    "unknown_type": "inplay_unk",
    "game_score": "game_score",
    "inherited_runners": "inherited_runners",
    "inherited_scored": "inherited_score",
    "wpa_pitch": "wpa_def",
    "avg_lvg_index": "leverage_index_avg",
    "re24_pitch": "re24_def",
}

PBP_STATS = {
    "pbp_table_row_number": "data-row",
    "score": "score_batting_team",
    "outs_before_play": "outs",
    "runners_on_base": "runners_on_bases_pbp",
    "team_batting_id_br": "batting_team_id",
    "play_description": "play_desc",
    "pitch_sequence": "pitches_pbp",
    "runs_outs_result": "runs_outs_result",
    "play_index_url": "play_index_url",
}

_GAME_ID_REGEX = re.compile(r"[A-Z]{3,3}\d{9,9}")
_TEAM_ID_REGEX = re.compile(r"[A-Z]{3,3}")
_W_L_SV_NAME_REGEX = re.compile(r"[LPSVW]{2,2}: (.+) (?:\(\d{1,2}-?\d{0,2}\))")
_ATTENDANCE_REGEX = re.compile(r"\d{1,2},\d{3,3}")
_GAME_DURATION_REGEX = re.compile(r"[1-9]:[0-5][0-9]")
_CHANGE_POS_REGEX = re.compile(r"from\s\b(?P<old_pos>\w+)\b\sto\s\b(?P<new_pos>\w+)\b")
_POS_REGEX = re.compile(r"\([BCDFHLPRS123]{1,2}\)")
_NUM_REGEX = re.compile(r"[1-9]{1}")
_INNING_TOTALS_REGEX = re.compile(
    r"(?P<runs>\d{1,2}) (run|runs), "
    r"(?P<hits>\d{1,2}) (hit|hits), "
    r"(?P<errors>\d{1,2}) (error|errors), "
    r"(?P<left_on_base>-?\d{1,2}) LOB."
    r"(?P<away_team_name>\s\b\w+\b){1,2} "
    r"(?P<away_team_runs>\d{1,2}),"
    r"(?P<home_team_name>\s\b\w+\b){1,2} "
    r"(?P<home_team_runs>\d{1,2})"
)


def parse_bbref_boxscore(scraped_html, url, game_id):
    """Parse boxscore data from the page source."""
    page_content = html.fromstring(scraped_html, base_url=url)
    (away_team_id, home_team_id) = _parse_team_ids(page_content, url)

    (
        away_team_bat_table,
        home_team_bat_table,
        away_team_pitch_table,
        home_team_pitch_table,
        play_by_play_table,
    ) = _parse_data_tables(page_content)

    (away_team_data, home_team_data) = _parse_all_team_data(
        page_content,
        away_team_bat_table,
        home_team_bat_table,
        away_team_pitch_table,
        home_team_pitch_table,
        away_team_id,
        home_team_id,
    )

    player_name_dict = _create_player_name_dict(
        away_team_bat_table,
        home_team_bat_table,
        away_team_pitch_table,
        home_team_pitch_table,
        away_team_id,
        home_team_id,
    )

    result = _parse_w_l_sv_pitchers(page_content, away_team_data, home_team_data, player_name_dict)
    if result.failure:
        return result

    result = _parse_game_meta_info(page_content)
    if result.failure:
        return result
    game_meta_info = result.value
    umpires = _parse_umpires(page_content)
    if not umpires:
        error = "Failed to parse umpire info"
        return Result.Fail(error)
    result = _parse_all_game_events(play_by_play_table, game_id, away_team_id, home_team_id, player_name_dict)
    if result.failure:
        error = f"Failed to parse innnings list:\n{result.error}"
        return Result.Fail(error)
    result_dict = result.value
    innings_list = result_dict["innings_list"]
    player_id_match_log = result_dict["player_id_match_log"]
    player_team_dict = result_dict["player_team_dict"]
    player_name_dict = {f"{name}, {team_id}": player_id for (name, team_id), player_id in player_name_dict.items()}

    boxscore = BBRefBoxscore(
        boxscore_url=url,
        bbref_game_id=game_id,
        game_meta_info=game_meta_info,
        away_team_data=away_team_data,
        home_team_data=home_team_data,
        innings_list=innings_list,
        umpires=umpires,
        player_id_match_log=player_id_match_log,
        player_team_dict=player_team_dict,
        player_name_dict=player_name_dict,
    )
    return Result.Ok(boxscore)


def _parse_team_ids(page_content, url):
    away_team_id = _parse_away_team_id(page_content)
    if not away_team_id:
        error = "Failed to parse away team ID"
        return Result.Fail(error)
    home_team_id = _parse_home_team_id(page_content)
    if not home_team_id:
        error = "Failed to parse home team ID"
        return Result.Fail(error)
    return (away_team_id, home_team_id)


def _parse_data_tables(page_content):
    result = page_content.xpath(_BAT_STATS_TABLE_XPATH)
    if not result or len(result) != 2:
        error = "Failed to parse batting stats table"
        return Result.Fail(error)
    away_team_bat_table = result[0]
    home_team_bat_table = result[1]

    result = page_content.xpath(_PITCH_STATS_TABLE_XPATH)
    if not result or len(result) != 2:
        error = "Failed to parse pitching stats table"
        return Result.Fail(error)
    away_team_pitch_table = result[0]
    home_team_pitch_table = result[1]

    result = page_content.xpath(_PBP_TABLE_XPATH)
    if not result:
        error = "Failed to parse play by play table"
        return Result.Fail(error)
    play_by_play_table = result[0]

    return (
        away_team_bat_table,
        home_team_bat_table,
        away_team_pitch_table,
        home_team_pitch_table,
        play_by_play_table,
    )


def _parse_away_team_id(page_content):
    name_urls = page_content.xpath(_TEAM_ID_XPATH)
    if not name_urls:
        return None
    matches = _TEAM_ID_REGEX.findall(name_urls[0])
    return matches[0] if matches else None


def _parse_home_team_id(page_content):
    name_urls = page_content.xpath(_TEAM_ID_XPATH)
    if not name_urls:
        return None
    matches = _TEAM_ID_REGEX.findall(name_urls[1])
    return matches[0] if matches else None


def _parse_all_team_data(
    page_content,
    away_team_bat_table,
    home_team_bat_table,
    away_team_pitch_table,
    home_team_pitch_table,
    away_team_id,
    home_team_id,
):
    result = _parse_team_data(
        page_content,
        team_batting_table=away_team_bat_table,
        team_pitching_table=away_team_pitch_table,
        team_id=away_team_id,
        opponent_id=home_team_id,
        is_home_team=False,
    )
    if result.failure:
        return result
    away_team_dict = result.value
    away_team_data = BBRefBoxscoreTeamData(**away_team_dict)

    result = _parse_team_data(
        page_content,
        team_batting_table=home_team_bat_table,
        team_pitching_table=home_team_pitch_table,
        team_id=home_team_id,
        opponent_id=away_team_id,
        is_home_team=True,
    )
    if result.failure:
        return result
    home_team_dict = result.value
    home_team_data = BBRefBoxscoreTeamData(**home_team_dict)

    return (away_team_data, home_team_data)


def _parse_team_data(page_content, team_batting_table, team_pitching_table, team_id, opponent_id, is_home_team):
    team_record = _parse_team_record(page_content, is_home_team)
    if not team_record:
        error = f"Failed to parse team record (team_id={team_id}, is_home_team={is_home_team})"
        return Result.Fail(error)
    team_totals = _parse_game_totals_for_team(page_content, team_id, is_home_team)
    if not team_totals:
        error = "Failed to parse team linescore totals (team_id={team_id}, is_home_team={is_home_team})"
        return Result.Fail(error)
    opponent_totals = _parse_game_totals_for_team(page_content, opponent_id, not is_home_team)
    if not opponent_totals:
        error = "Failed to parse opponent linescore totals (team_id={opponent_id}, is_home_team={not is_home_team})"
        return Result.Fail(error)
    batting_stats = _parse_batting_stats_for_team(team_batting_table, team_id, opponent_id)
    if not batting_stats:
        error = "Failed to parse away team batting stats"
        return Result.Fail(error)
    pitching_stats = _parse_pitching_stats_for_team(team_pitching_table, team_id, opponent_id)
    if not pitching_stats:
        error = "Failed to parse away team pitching stats"
        return Result.Fail(error)
    result = _parse_starting_lineup_for_team(page_content, team_id, is_home_team)
    if result.failure:
        return result
    starting_lineup = result.value

    team_dict = {
        "team_id_br": team_id,
        "total_wins_before_game": int(team_record[0]),
        "total_losses_before_game": int(team_record[1]),
        "total_runs_scored_by_team": int(team_totals["R"]),
        "total_runs_scored_by_opponent": int(opponent_totals["R"]),
        "total_hits_by_team": int(team_totals["H"]),
        "total_hits_by_opponent": int(opponent_totals["H"]),
        "total_errors_by_team": int(team_totals["E"]),
        "total_errors_by_opponent": int(opponent_totals["E"]),
        "batting_stats": batting_stats,
        "pitching_stats": pitching_stats,
        "starting_lineup": starting_lineup,
    }
    return Result.Ok(team_dict)


def _parse_team_record(page_content, is_home_team):
    if is_home_team:
        team_record_xpath = _HOME_TEAM_RECORD_XPATH
    else:
        team_record_xpath = _AWAY_TEAM_RECORD_XPATH
    team_record_dirty = page_content.xpath(team_record_xpath)
    if not team_record_dirty:
        return None
    team_record = team_record_dirty[0].split("-")
    if len(team_record) != 2:
        return None
    return team_record


def _parse_game_totals_for_team(page_content, team_id, is_home_team):
    if is_home_team:
        linescore_xpath = _LINESCORE_HOME_VALS_XPATH
    else:
        linescore_xpath = _LINESCORE_AWAY_VALS_XPATH

    keys = page_content.xpath(_LINESCORE_KEYS_XPATH)
    vals = page_content.xpath(linescore_xpath)
    if not keys or not vals:
        return None
    if len(keys) < 2 or len(vals) < 2:
        return None
    del keys[0:2]
    del vals[0:2]
    if len(keys) < 3 or len(vals) < 3:
        return None
    del keys[:-3]
    del vals[:-3]
    if len(keys) != len(vals):
        return None
    return dict(zip(keys, vals))


def _parse_batting_stats_for_team(team_batting_table, player_team_id, opponent_team_id):
    bat_data = []
    for player_id in team_batting_table.xpath(_BATTER_IDS_XPATH):
        player_bat_stats_xpath = Template(_T_BAT_STATS_ROW_XPATH).substitute(pid=player_id)
        results = team_batting_table.xpath(player_bat_stats_xpath)
        if not results:
            return []
        stat_dict = {attr: _get_bat_stat_value(results[0], stat_name) for attr, stat_name in BAT_STATS.items()}
        stat_dict["player_id_br"] = player_id
        stat_dict["player_team_id_br"] = player_team_id
        stat_dict["opponent_team_id_br"] = opponent_team_id
        stat_dict["details"] = _parse_batting_details(stat_dict)
        player_data = BBRefBatStats(**stat_dict)
        bat_data.append(player_data)
    return bat_data


def _get_bat_stat_value(player_bat_data_html, stat_name):
    bat_stat_xpath = Template(_T_BAT_STATS_XPATH).substitute(stat=stat_name)
    bat_stat_value = "0"
    if stat_name == "details":
        bat_stat_value = []
    result = player_bat_data_html.xpath(bat_stat_xpath)
    if result and len(result) > 0:
        bat_stat_value = result[0]
    return bat_stat_value if isinstance(bat_stat_value, list) else bat_stat_value.rstrip("%")


def _parse_batting_details(stat_dict):
    details_str = stat_dict["details"]
    if not details_str:
        return []
    details = []
    for s in details_str.split(","):
        d = BBRefBatStatsDetail()
        t = s.split("\u00b7")
        if len(t) == 1:
            d.count = "1"
            d.stat = t[0].strip("\n")
            details.append(d)
        if len(t) == 2:
            d.count = t[0][0]
            d.stat = t[1].strip("\n")
            details.append(d)
    return details


def _parse_pitching_stats_for_team(team_pitching_table, player_team_id, opponent_team_id):
    pitch_data = []
    for player_id in team_pitching_table.xpath(_PITCHER_IDS_XPATH):
        player_pitch_stats_xpath = Template(_T_PITCH_STATS_ROW_XPATH).substitute(pid=player_id)
        results = team_pitching_table.xpath(player_pitch_stats_xpath)
        if not results:
            return None
        stat_dict = {attr: _get_pitch_stat_value(results[0], stat_name) for attr, stat_name in PITCH_STATS.items()}
        stat_dict["player_id_br"] = player_id
        stat_dict["player_team_id_br"] = player_team_id
        stat_dict["opponent_team_id_br"] = opponent_team_id
        player_data = BBRefPitchStats(**stat_dict)
        pitch_data.append(player_data)
    return pitch_data


def _get_pitch_stat_value(player_pitch_data_html, stat_name):
    pitch_stat_xpath = Template(_T_PITCH_STAT_XPATH).substitute(stat=stat_name)
    pitch_stat_value = "0"
    result = player_pitch_data_html.xpath(pitch_stat_xpath)
    if result and len(result) > 0:
        pitch_stat_value = result[0]
    return pitch_stat_value if isinstance(pitch_stat_value, list) else pitch_stat_value.rstrip("%")


def _parse_starting_lineup_for_team(page_content, team_id, is_home_team):
    lineup = []
    if is_home_team:
        bat_orders = page_content.xpath(_HOME_LINEUP_ORDER_XPATH)
        player_links = page_content.xpath(_HOME_LINEUP_PLAYER_XPATH)
        def_positions = page_content.xpath(_HOME_LINEUP_DEF_POS_XPATH)
    else:
        bat_orders = page_content.xpath(_AWAY_LINEUP_ORDER_XPATH)
        player_links = page_content.xpath(_AWAY_LINEUP_PLAYER_XPATH)
        def_positions = page_content.xpath(_AWAY_LINEUP_DEF_POS_XPATH)
    if not bat_orders or not player_links or not def_positions:
        error = "Failed to parse team lineup data (team_id={team_id}, is_home_team={is_home_team})"
        return Result.Fail(error)
    for i in range(len(bat_orders)):
        bat = BBRefStartingLineupSlot()
        split = player_links[i].split("/")
        if len(split) != 4:
            error = (
                "Player links in lineup table are not in the expected format "
                f"(team_id={team_id}, is_home_team={is_home_team})"
            )
            return Result.Fail(error)
        bat.player_id_br = split[3][:-6]
        bat.bat_order = bat_orders[i]
        bat.def_position = def_positions[i]
        lineup.append(bat)
    return Result.Ok(lineup)


def _parse_w_l_sv_pitchers(page_content, away_team_data, home_team_data, player_name_dict):
    pitcher_names = _parse_w_l_sv_save_pitcher_names(page_content)
    if not pitcher_names["wp"] or not pitcher_names["lp"]:
        return Result.Fail("Failed to parse the names of the WP and LP")
    winning_team_id = away_team_data.team_id_br if away_team_data.team_won else home_team_data.team_id_br
    losing_team_id = away_team_data.team_id_br if home_team_data.team_won else home_team_data.team_id_br
    w_pitcher_id_br = _match_player_id(pitcher_names["wp"], winning_team_id, player_name_dict)["id"]
    l_pitcher_id_br = _match_player_id(pitcher_names["lp"], losing_team_id, player_name_dict)["id"]
    sv_pitcher_id_br = (
        _match_player_id(pitcher_names["sv"], winning_team_id, player_name_dict)["id"] if pitcher_names["sv"] else ""
    )
    if away_team_data.team_won:
        away_team_data.pitcher_of_record = w_pitcher_id_br
        away_team_data.pitcher_earned_save = sv_pitcher_id_br
        home_team_data.pitcher_of_record = l_pitcher_id_br
    else:
        away_team_data.pitcher_of_record = l_pitcher_id_br
        home_team_data.pitcher_of_record = w_pitcher_id_br
        home_team_data.pitcher_earned_save = sv_pitcher_id_br
    return Result.Ok()


def _parse_w_l_sv_save_pitcher_names(page_content):
    pitchers = page_content.xpath(_LINESCORE_W_L_SV_XPATH)
    if not pitchers:
        return None
    pitchers = pitchers[0].strip().replace("\xa0", " ")
    pitcher_list = pitchers.split(" • ")
    if len(pitcher_list) not in [2, 3]:
        return None
    wp_match = _W_L_SV_NAME_REGEX.search(pitcher_list[0])
    lp_match = _W_L_SV_NAME_REGEX.search(pitcher_list[1])
    sv_match = _W_L_SV_NAME_REGEX.search(pitcher_list[2]) if len(pitcher_list) == 3 else None
    return {
        "wp": wp_match.group(1) if wp_match else "",
        "lp": lp_match.group(1) if lp_match else "",
        "sv": sv_match.group(1) if sv_match else "",
    }


def _parse_game_meta_info(page_content):
    scorebox_meta_strings = page_content.xpath(_SCOREBOX_META_XPATH)
    attendance_matches = _parse_attendance_from_strings(scorebox_meta_strings)
    if attendance_matches is not None:
        attendance = attendance_matches["match"]
        attendance_index = attendance_matches["index"]
        del scorebox_meta_strings[attendance_index]
    else:
        attendance = "0"

    venue_matches = _parse_venue_from_strings(scorebox_meta_strings)
    if venue_matches is None:
        error = "Failed to parse park name"
        return Result.Fail(error)

    park_name = venue_matches["match"]
    venue_index = venue_matches["index"]
    del scorebox_meta_strings[venue_index]

    game_duration_matches = _parse_game_duration_from_strings(scorebox_meta_strings)
    if game_duration_matches is None:
        error = "Failed to parse game duration"
        return Result.Fail(error)

    game_duration = game_duration_matches["match"]
    game_duration_index = game_duration_matches["index"]
    del scorebox_meta_strings[game_duration_index]

    day_night_field = _parse_day_night_field_type_from_strings(scorebox_meta_strings)
    if day_night_field is None:
        error = "Failed to parse game time/field type"
        return Result.Fail(error)

    split = day_night_field["match"].split(",")
    day_night_field_index = day_night_field["index"]
    del scorebox_meta_strings[day_night_field_index]

    if len(split) < 2:
        error = "Failed to parse game time/field type"
        return Result.Fail(error)

    day_night = split[0].strip()
    field_type = split[1].strip().title()

    first_pitch_weather = page_content.xpath(_FIRST_PITCH_WEATHER_XPATH)
    if not first_pitch_weather:
        error = "Failed to parse first pitch weather info"
        return Result.Fail(error)
    split2 = first_pitch_weather[0].split(",")
    if len(split2) < 2:
        error = f"First pitch weather info is not in expected format:\n{first_pitch_weather}"
        return Result.Fail(error)

    first_pitch_temperature = split2[0].strip()[:2]
    first_pitch_wind = split2[1].strip()

    first_pitch_clouds = ""
    if len(split2) > 2:
        first_pitch_clouds = split2[2].strip().strip(".")

    first_pitch_precipitation = ""
    if len(split2) > 3:
        first_pitch_precipitation = split2[3].strip().strip(".")

    game_info = BBRefBoxscoreMeta(
        attendance=int(attendance),
        park_name=park_name,
        game_duration=game_duration,
        day_night=day_night,
        field_type=field_type,
        first_pitch_temperature=int(first_pitch_temperature),
        first_pitch_wind=first_pitch_wind,
        first_pitch_clouds=first_pitch_clouds,
        first_pitch_precipitation=first_pitch_precipitation,
    )
    return Result.Ok(game_info)


def _parse_attendance_from_strings(strings):
    attendance = []
    for i in range(len(strings)):
        matches = _ATTENDANCE_REGEX.findall(strings[i])
        if matches:
            for m in matches:
                d = {"match": m.replace(",", ""), "index": i}
                attendance.append(d)
    if len(attendance) == 1:
        return attendance[0]
    return None


def _parse_venue_from_strings(strings):
    for i in range(len(strings)):
        for t in VENUE_TERMS:
            if t in strings[i].lower():
                return {"match": strings[i][2:], "index": i}
    return None


def _parse_game_duration_from_strings(strings):
    duration = []
    for i in range(len(strings)):
        if "start time" in strings[i].lower():
            continue
        matches = _GAME_DURATION_REGEX.findall(strings[i])
        if matches:
            for m in matches:
                d = {"match": m, "index": i}
                duration.append(d)
    if len(duration) == 1:
        return duration[0]
    return None


def _parse_day_night_field_type_from_strings(strings):
    matches = []
    for i in range(len(strings)):
        if "game, on" in strings[i].lower():
            d = {"match": strings[i], "index": i}
            matches.append(d)
    if len(matches) == 1:
        return matches[0]
    return None


def _parse_umpires(page_content):
    umpires = page_content.xpath(_UMPIRES_XPATH)[0]
    if umpires is None:
        return None
    split = umpires.split(",")
    umps = []
    for s in split:
        u = BBRefUmpire()
        split2 = s.strip(".").strip().split("-")
        if len(split2) != 2:
            return None
        u.field_location = split2[0].strip()
        u.umpire_name = split2[1].strip()
        umps.append(u)
    return umps


def _create_player_name_dict(
    away_team_bat_table,
    home_team_bat_table,
    away_team_pitch_table,
    home_team_pitch_table,
    away_team_id,
    home_team_id,
):
    away_team_batter_name_dict = _parse_batter_name_dict(away_team_bat_table, away_team_id)
    if not away_team_batter_name_dict:
        error = "Failed to parse away team batter name dictionary"
        return Result.Fail(error)
    home_team_batter_name_dict = _parse_batter_name_dict(home_team_bat_table, home_team_id)
    if not home_team_batter_name_dict:
        error = "Failed to parse home team batter name dictionary"
        return Result.Fail(error)
    away_team_pitcher_name_dict = _parse_pitcher_name_dict(away_team_pitch_table, away_team_id)
    if not away_team_pitcher_name_dict:
        error = "Failed to parse away team pitcher name dictionary"
        return Result.Fail(error)
    home_team_pitcher_name_dict = _parse_pitcher_name_dict(home_team_pitch_table, home_team_id)
    if not home_team_pitcher_name_dict:
        error = "Failed to parse home team pitcher name dictionary"
        return Result.Fail(error)
    batter_name_dict = {**away_team_batter_name_dict, **home_team_batter_name_dict}
    pitcher_name_dict = {**away_team_pitcher_name_dict, **home_team_pitcher_name_dict}
    return {**batter_name_dict, **pitcher_name_dict}


def _parse_batter_name_dict(team_batting_table, team_id_br):
    batter_ids = team_batting_table.xpath(_BATTER_IDS_XPATH)
    batter_names = team_batting_table.xpath(_BATTER_NAMES_XPATH)
    if not batter_ids or not batter_names:
        return None
    if len(batter_ids) != len(batter_names):
        return None
    dict_keys = [(name, team_id_br) for name in batter_names]
    return dict(zip(dict_keys, batter_ids))


def _parse_pitcher_name_dict(team_pitching_table, team_id_br):
    pitcher_ids = team_pitching_table.xpath(_PITCHER_IDS_XPATH)
    pitcher_names = team_pitching_table.xpath(_PITCHER_NAMES_XPATH)
    if not pitcher_ids or not pitcher_names:
        return None
    if len(pitcher_ids) != len(pitcher_names):
        return None
    dict_keys = [(name, team_id_br) for name in pitcher_names]
    return dict(zip(dict_keys, pitcher_ids))


def _parse_all_game_events(play_by_play_table, game_id, away_team_id, home_team_id, player_name_dict):
    inning_summaries_top = _parse_inning_summary_top(play_by_play_table)
    if not inning_summaries_top:
        error = "Failed to parse inning start summaries"
        return Result.Fail(error)

    inning_summaries_bottom = _parse_inning_summary_bottom(play_by_play_table)
    if not inning_summaries_bottom:
        error = "Failed to parse inning end summaries"
        return Result.Fail(error)

    result = _parse_play_by_play(play_by_play_table, player_name_dict, away_team_id, home_team_id, game_id)
    if result.failure:
        error = f"rescrape_did_not_parse_play_by_play:\n{result.error}"
        return Result.Fail(error)
    result_dict = result.value
    play_by_play_events = result_dict["play_by_play"]
    player_id_match_log = list(result_dict["player_id_match_log"])
    player_team_dict = _create_player_team_dict(play_by_play_events)
    if not player_team_dict:
        error = "Player name was unmatched in play by play events"
        return Result.Fail(error)

    result = _parse_in_game_substitutions(play_by_play_table, game_id, player_name_dict)
    if result.failure:
        error = f"Failed to parse in game substitutions:\n{result.error}"
        return Result.Fail(error)
    result_dict = result.value
    in_game_substitutions = result_dict["sub_list"]

    misc_events = []
    missing_row_ids = _find_missing_pbp_events(
        inning_summaries_top,
        inning_summaries_bottom,
        play_by_play_events,
        in_game_substitutions,
    )
    if missing_row_ids:
        misc_events = _parse_missing_pbp_events(missing_row_ids, play_by_play_table, game_id)

    result = _create_innings_list(
        game_id,
        away_team_id,
        home_team_id,
        player_name_dict,
        player_id_match_log,
        inning_summaries_top,
        inning_summaries_bottom,
        play_by_play_events,
        in_game_substitutions,
        misc_events,
    )
    if result.failure:
        error = f"Error occurred constructing inning list:\n{result.error}"
        return Result.Fail(error)
    (innings_list, player_id_match_log) = result.value
    result_dict = {
        "innings_list": innings_list,
        "player_id_match_log": player_id_match_log,
        "player_team_dict": player_team_dict,
    }
    return Result.Ok(result_dict)


def _parse_inning_summary_top(play_by_play_table):
    summaries = play_by_play_table.xpath(_PBP_INN_SUM_TOP_XPATH)
    if summaries is None:
        return None
    inning_summaries = [s.strip().replace("\xa0", " ") for s in summaries]
    row_numbers = play_by_play_table.xpath(_PBP_INN_SUM_TOP_ROW_NUM_XPATH)
    if row_numbers is None:
        return None
    summary_row_numbers = [int(s) for s in row_numbers]
    if len(inning_summaries) != len(summary_row_numbers):
        return None
    return dict(zip(summary_row_numbers, inning_summaries))


def _parse_inning_summary_bottom(play_by_play_table):
    summaries = play_by_play_table.xpath(_PBP_INN_SUM_BOT_XPATH)
    if summaries is None:
        return None
    inning_summaries = [s.strip() for s in summaries]
    last_summary = play_by_play_table.xpath(_PBP_INN_SUM_BOT_LAST_XPATH)
    for s in last_summary:
        inning_summaries.append(s.strip())
    row_numbers = play_by_play_table.xpath(_PBP_INN_SUM_BOT_ROW_NUM_XPATH)
    if row_numbers is None:
        return None
    summary_row_numbers = [int(s) for s in row_numbers]
    if len(inning_summaries) != len(summary_row_numbers):
        return None
    return dict(zip(summary_row_numbers, inning_summaries))


def _parse_play_by_play(pbp_table, player_id_dict, away_team_id, home_team_id, game_id):
    play_by_play = []
    player_id_match_log = []
    inning_list = pbp_table.xpath(_PBP_INNING_XPATH)
    for i in range(len(inning_list)):
        event_num = i + 1
        event_dict = _parse_pbp_event(pbp_table, event_num)
        inning_label = inning_list[i]
        inning_number = int(inning_label[-1])
        inning_id = f"{game_id}_INN_TOP{inning_number:02d}"
        event_dict["inning_label"] = inning_label
        event_dict["inning_id"] = inning_id

        if event_dict["team_batting_id_br"] == away_team_id:
            event_dict["team_pitching_id_br"] = home_team_id
        else:
            event_dict["team_pitching_id_br"] = away_team_id

        batter = _get_pbp_event_stat_value(pbp_table, "batter", event_num).replace("\xa0", " ")
        match = _match_player_id(batter, event_dict["team_batting_id_br"], player_id_dict)
        if match["type"] != "Exact match":
            player_id_match_log.append(match)
        event_dict["batter_id_br"] = match["id"]

        pitcher = _get_pbp_event_stat_value(pbp_table, "pitcher", event_num).replace("\xa0", " ")
        match = _match_player_id(pitcher, event_dict["team_pitching_id_br"], player_id_dict)
        if match["type"] != "Exact match":
            player_id_match_log.append(match)
        event_dict["pitcher_id_br"] = match["id"]

        if event_dict["play_index_url"]:
            event_dict["play_index_url"] = "https://www.baseball-reference.com" + event_dict["play_index_url"]
            event_dict["event_id"] = url_query_parameter(event_dict["play_index_url"], "game-event")

        event = BBRefPlayByPlayEvent(**event_dict)
        play_by_play.append(event)

    result = {"play_by_play": play_by_play, "player_id_match_log": player_id_match_log}
    return Result.Ok(result)


def _parse_pbp_event(pbp_table, event_number):
    return {
        attr: _get_pbp_event_stat_value(pbp_table, stat_name, event_number) for attr, stat_name in PBP_STATS.items()
    }


def _get_pbp_event_stat_value(pbp_table, stat_name, event_number):
    templ_xpath = './tbody//tr[@id="event_${n}"]/td[@data-stat="${s}"]/text()'
    query = Template(templ_xpath).substitute(n=event_number, s=stat_name)

    if stat_name == "pitches_pbp":
        templ_xpath = (
            './tbody//tr[@id="event_${n}"]/td[@data-stat="pitches_pbp"]' '/span[@class="pitch_sequence"]/text()'
        )
        query = Template(templ_xpath).substitute(n=event_number)

    if stat_name == "data-row":
        templ_xpath = './tbody//tr[@id="event_${n}"]/@data-row'
        query = Template(templ_xpath).substitute(n=event_number)

    if stat_name == "play_index_url":
        templ_xpath = './tbody//tr[@id="event_${n}"]/td[@data-stat="runners_on_bases_pbp"]/@data-endpoint'
        query = Template(templ_xpath).substitute(n=event_number)

    pbp_value = ""
    result = pbp_table.xpath(query)
    if result and len(result) > 0:
        pbp_value = result[0]
    return pbp_value


def _match_player_id(name, team_id, id_dict):
    match = {}
    if (name, team_id) in id_dict:
        match["type"] = "Exact match"
        match["name"] = name
        match["team_id"] = team_id
        match["id"] = id_dict[(name, team_id)]
        match["score"] = 1
    else:
        mapped_choices = {player_id: player_name for (player_name, team_id), player_id in id_dict.items()}
        best_matches = fuzzy_match(name, mapped_choices)
        player_id = best_matches[0]["result"]
        name_dict = {v: k for k, v in id_dict.items()}
        match["type"] = "Fuzzy match"
        match["name"] = name
        match["team_id"] = team_id
        match["best_match"] = name_dict[player_id]
        match["id"] = player_id
        match["score"] = best_matches[0]["score"]
    return match


def _create_player_team_dict(play_by_play):
    ids = []
    teams = []
    for pbp in play_by_play:
        ids.append(pbp.batter_id_br)
        ids.append(pbp.pitcher_id_br)
        teams.append(pbp.team_batting_id_br)
        teams.append(pbp.team_pitching_id_br)

    player_dict = dict(zip(ids, teams))
    for id in player_dict:
        if len(id) == 0:
            return None
    return player_dict


def _parse_in_game_substitutions(play_by_play_table, game_id, player_name_dict):
    row_numbers = play_by_play_table.xpath(_PBP_PLAYER_SUB_ROW_NUM_XPATH)
    if row_numbers is None:
        error = "No in game substitutions found in play-by-play table"
        return Result.Fail(error)
    sub_list = []
    for n in row_numbers:
        row_num = int(n)
        subs_xpath = Template(_T_PBP_PLAYER_SUB_XPATH).substitute(row=row_num)
        sub_descriptions = play_by_play_table.xpath(subs_xpath)
        if sub_descriptions is None:
            continue
        for i in range(len(sub_descriptions)):
            s = sub_descriptions[i].strip().replace("\xa0", " ")
            result = _parse_substitution_description(s)
            if result.failure:
                return result
            sub_dict = result.value
            substitution = BBRefInGameSubstitution(
                pbp_table_row_number=row_num,
                sub_description=sub_dict["description"],
                incoming_player_name=sub_dict["incoming_player_name"],
                outgoing_player_name=sub_dict["outgoing_player_name"],
                incoming_player_pos=sub_dict["incoming_player_pos"],
                outgoing_player_pos=sub_dict["outgoing_player_pos"],
                lineup_slot=sub_dict["lineup_slot"],
                sub_type=sub_dict["sub_type"],
                team_id="",
            )
            sub_list.append(substitution)

    result = {"sub_list": sub_list}
    return Result.Ok(result)


def _parse_substitution_description(sub_description):
    parsed_sub = {"description": sub_description}
    split, split2 = None, None
    pre_split = [s.strip() for s in sub_description.split("(change occurred mid-batter)")]
    sub_description = pre_split[0]
    if "running at second base to start the extra inning" in sub_description:
        des = "running at second base to start the extra inning"
        split = [s.strip() for s in sub_description.split(des)]
        parsed_sub["incoming_player_name"] = split[0]
        parsed_sub["incoming_player_pos"] = "PR"
        parsed_sub["outgoing_player_name"] = "N/A"
        parsed_sub["outgoing_player_pos"] = "N/A"
        parsed_sub["lineup_slot"] = 0
        parsed_sub["sub_type"] = "bat"
        remaining_description = split[1]
        if not remaining_description:
            return Result.Ok(parsed_sub)
    if "replaces" in sub_description:
        split = [s.strip() for s in sub_description.split("replaces")]
        parsed_sub["sub_type"] = "pitch"
    elif "and is now" in sub_description:
        split = [s.strip() for s in sub_description.split("pinch hit for")]
        parsed_sub["incoming_player_name"] = split[0]
        remaining_description = split[1]
        split2 = [s.strip() for s in remaining_description.split("and is now")]
        parsed_sub["incoming_player_pos"] = split[0]
        parsed_sub["outgoing_player_pos"] = split[0]
        parsed_sub["outgoing_player_name"] = "N/A"
        parsed_sub["lineup_slot"] = 0
        parsed_sub["sub_type"] = "bat"
        return Result.Ok(parsed_sub)
    elif "pinch hits for" in sub_description:
        parsed_sub["incoming_player_pos"] = "PH"
        parsed_sub["sub_type"] = "bat"
        split = [s.strip() for s in sub_description.split("pinch hits for")]
    elif "pinch runs for" in sub_description:
        parsed_sub["incoming_player_pos"] = "PR"
        parsed_sub["sub_type"] = "bat"
        split = [s.strip() for s in sub_description.split("pinch runs for")]
    elif "moves" in sub_description:
        split = [s.strip() for s in sub_description.split("moves")]
        parsed_sub["sub_type"] = "pitch"
    else:
        error = "Substitution description was in an unrecognized format. (Before first split)"
        return Result.Fail(error)

    if not split or len(split) != 2:
        error = "First split operation did not produce a list with length=2."
        return Result.Fail(error)
    parsed_sub["incoming_player_name"] = split[0]
    remaining_description = split[1]

    if "batting" in remaining_description:
        split2 = [s.strip() for s in remaining_description.split("batting")]
    elif "from" in remaining_description:
        match = _CHANGE_POS_REGEX.search(remaining_description)
        if match:
            match_dict = match.groupdict()
            parsed_sub["outgoing_player_name"] = parsed_sub["incoming_player_name"]
            parsed_sub["outgoing_player_pos"] = match_dict["old_pos"]
            parsed_sub["incoming_player_pos"] = match_dict["new_pos"]
            parsed_sub["lineup_slot"] = 0
            return Result.Ok(parsed_sub)
    elif remaining_description.endswith("pitching"):
        split2 = [s.strip() for s in remaining_description.split("pitching")]
        parsed_sub["outgoing_player_name"] = split2[0]
        parsed_sub["outgoing_player_pos"] = "P"
        parsed_sub["incoming_player_pos"] = "P"
        parsed_sub["lineup_slot"] = 0
        return Result.Ok(parsed_sub)
    else:
        error = "Substitution description was in an unrecognized format. (After first split)"
        return Result.Fail(error)

    if not split2 or len(split2) != 2:
        error = "Second split operation did not produce a list with length=2."
        return Result.Fail(error)
    remaining_description = split2[0]
    lineup_slot_str = split2[1]

    match = _NUM_REGEX.search(lineup_slot_str)
    if not match:
        error = "Failed to parse batting order number."
        return Result.Fail(error)
    parsed_sub["lineup_slot"] = int(match.group())

    split3 = None
    match = _POS_REGEX.search(remaining_description)
    if match:
        parsed_sub["outgoing_player_pos"] = match.group()[1:-1]
        split3 = [s.strip() for s in remaining_description.split(match.group())]
        if not split3 or len(split3) != 2:
            error = "Third split operation did not produce a list with length=2."
            return Result.Fail(error)
        parsed_sub["outgoing_player_name"] = split3[0]
        remaining_description = split3[1]
    if not remaining_description:
        return Result.Ok(parsed_sub)

    split4 = None
    if "pitching" in remaining_description:
        parsed_sub["incoming_player_pos"] = "P"
        parsed_sub["sub_type"] = "pitch"
        split4 = [s.strip() for s in remaining_description.split("pitching")]
        if not split4 or len(split4) != 2:
            error = "Fourth split operation did not produce a list with length=2."
            return Result.Fail(error)
        if "outgoing_player_name" not in parsed_sub:
            parsed_sub["outgoing_player_name"] = split4[0]
        remaining_description = split4[1]

    elif "playing" in remaining_description:
        split4 = [s.strip() for s in remaining_description.split("playing")]
        if not split4 or len(split4) != 2:
            error = "Fourth split operation did not produce a list with length=2."
            return Result.Fail(error)
        if "outgoing_player_name" not in parsed_sub:
            parsed_sub["outgoing_player_name"] = split4[0]
        remaining_description = split4[1]

        found_incoming_player_pos = False
        for pos in DEFENSE_POSITIONS:
            if pos in remaining_description:
                parsed_sub["incoming_player_pos"] = pos
                found_incoming_player_pos = True
                break
        if not found_incoming_player_pos:
            error = (
                "Unable to find incoming player position in remaining description:\n"
                f"remaining_description: {remaining_description}\n"
            )
            return Result.Fail(error)
    elif "pinch hit" in sub_description:
        parsed_sub["outgoing_player_name"] = remaining_description
        parsed_sub["outgoing_player_pos"] = "PH"
        parsed_sub["sub_type"] = "bat"
    elif "pinch run" in sub_description:
        parsed_sub["outgoing_player_name"] = remaining_description
        parsed_sub["outgoing_player_pos"] = "PR"
        parsed_sub["sub_type"] = "bat"

    if "outgoing_player_pos" not in parsed_sub:
        parsed_sub["outgoing_player_pos"] = parsed_sub["incoming_player_pos"]
    return Result.Ok(parsed_sub)


def _find_missing_pbp_events(summaries_begin, summaries_end, game_events, substitutions):
    row_ids_top_inning = list(summaries_begin.keys())
    row_ids_bot_inning = list(summaries_end.keys())
    row_ids_game_events = [int(event.pbp_table_row_number) for event in game_events]
    row_ids_substitutions = [sub.pbp_table_row_number for sub in substitutions]
    all_row_ids = set(row_ids_top_inning + row_ids_bot_inning + row_ids_game_events + row_ids_substitutions)
    first_row_num = min(all_row_ids)
    last_row_num = max(all_row_ids) + 1
    check_row_ids = set(range(first_row_num, last_row_num))
    missing_row_ids = check_row_ids - all_row_ids
    return list(missing_row_ids)


def _parse_missing_pbp_events(missing_row_ids, play_by_play_table, game_id):
    misc_events = []
    for row_num in missing_row_ids:
        missing_events_xpath = Template(_T_PBP_MISC_EVENT_XPATH).substitute(row=row_num)
        event_descriptions = play_by_play_table.xpath(missing_events_xpath)
        if event_descriptions is None:
            continue
        description = "".join(des.strip().replace("\xa0", " ") for des in event_descriptions)

        misc_event = BBRefPlayByPlayMiscEvent(pbp_table_row_number=row_num, description=description)
        misc_events.append(misc_event)
    return misc_events


def _create_innings_list(
    game_id,
    away_team_id,
    home_team_id,
    player_name_dict,
    player_id_match_log,
    summaries_begin,
    summaries_end,
    game_events,
    substitutions,
    misc_events,
):
    if len(summaries_begin) != len(summaries_end):
        error = "Begin inning and end inning summary list lengths do not match."
        return Result.Fail(error)

    start_boundaries = sorted(summaries_begin.keys())
    end_boundaries = sorted(summaries_end.keys())
    inning_number = 0
    innings_list = []
    for i in range(len(summaries_begin)):
        start_row = start_boundaries[i]
        end_row = end_boundaries[i]
        if is_even(i):
            inning_number += 1
            inning_label = f"t{inning_number}"
            inning_id = f"{game_id}_INN_TOP{inning_number:02d}"
        else:
            inning_label = f"b{inning_number}"
            inning_id = f"{game_id}_INN_BOT{inning_number:02d}"

        inning_events = [
            g for g in game_events if int(g.pbp_table_row_number) > start_row and int(g.pbp_table_row_number) < end_row
        ]
        for game_event in inning_events:
            game_event.inning_id = inning_id
            game_event.inning_label = inning_label

        inning_substitutions = [
            sub for sub in substitutions if sub.pbp_table_row_number > start_row and sub.pbp_table_row_number < end_row
        ]
        for sub in inning_substitutions:
            sub.inning_id = inning_id
            sub.inning_label = inning_label
            sub.team_id = _get_sub_team_id(inning_label, sub.sub_type, away_team_id, home_team_id)
            result = _get_sub_player_ids(
                sub.incoming_player_name,
                sub.outgoing_player_name,
                player_name_dict,
                sub.team_id,
            )
            if result.failure:
                return result
            id_dict = result.value
            sub.incoming_player_id_br = id_dict["incoming_player_id_br"]
            sub.outgoing_player_id_br = id_dict["outgoing_player_id_br"]
            if id_dict["player_id_match_log"]:
                player_id_match_log.extend(id_dict["player_id_match_log"])

        misc = [
            misc_event
            for misc_event in misc_events
            if misc_event.pbp_table_row_number > start_row and misc_event.pbp_table_row_number < end_row
        ]
        for me in misc:
            me.inning_id = inning_id
            me.inning_label = inning_label

        inning = BBRefHalfInning()
        inning.inning_id = inning_id
        inning.inning_label = inning_label
        inning.begin_inning_summary = summaries_begin[start_row]
        inning.end_inning_summary = summaries_end[end_row]
        inning.game_events = inning_events
        inning.substitutions = inning_substitutions
        inning.misc_events = misc

        match = _INNING_TOTALS_REGEX.search(summaries_end[end_row])
        if match:
            inning_totals = match.groupdict()
            inning.inning_total_runs = inning_totals["runs"]
            inning.inning_total_hits = inning_totals["hits"]
            inning.inning_total_errors = inning_totals["errors"]
            inning.inning_total_left_on_base = inning_totals["left_on_base"]
            inning.away_team_runs_after_inning = inning_totals["away_team_runs"]
            inning.home_team_runs_after_inning = inning_totals["home_team_runs"]

        innings_list.append(inning)
    return Result.Ok((innings_list, player_id_match_log))


def _get_sub_team_id(inning_label, sub_type, away_team_id, home_team_id):
    if inning_label.startswith("t") and "bat" in sub_type:
        return away_team_id
    elif inning_label.startswith("t") and "pitch" in sub_type:
        return home_team_id
    elif inning_label.startswith("b") and "bat" in sub_type:
        return home_team_id
    elif inning_label.startswith("b") and "pitch" in sub_type:
        return away_team_id
    return None


def _get_sub_player_ids(incoming_player_name, outgoing_player_name, player_name_dict, sub_team_id):
    player_id_match_log = []
    if outgoing_player_name != "N/A":
        match = _match_player_id(outgoing_player_name, sub_team_id, player_name_dict)
        outgoing_player_id_br = match["id"]
        if match["type"] != "Exact match":
            player_id_match_log.append(match)
    else:
        outgoing_player_id_br = "N/A"

    if incoming_player_name != "N/A":
        match = _match_player_id(incoming_player_name, sub_team_id, player_name_dict)
        incoming_player_id_br = match["id"]
        if match["type"] != "Exact match":
            player_id_match_log.append(match)
    else:
        incoming_player_id_br = "N/A"

    result = {
        "incoming_player_id_br": incoming_player_id_br,
        "outgoing_player_id_br": outgoing_player_id_br,
        "player_id_match_log": player_id_match_log,
    }
    return Result.Ok(result)
