import re
from string import Template

from lxml import html

from vigorish.scrape.brooks_pitch_logs.models.pitch_log import BrooksPitchLog
from vigorish.util.result import Result

PITCHFX_URL_XPATH = '//a[text()="Get Expanded Tabled Data"]/@href'
PITCH_COUNTS_XPATH = '//div[@class="span9"]//table[2]//tr'

T_PITCHFX_URL = "http://www.brooksbaseball.net/pfxVB/${rel_url}"
T_PITCHER_NAME_XPATH = '//option[@value="${id}"]/text()'
T_INNING_NUM_XPATH = '//div[@class="span9"]//table[2]//tr[${index}]/td[1]/text()'
T_PITCH_COUNT_XPATH = '//div[@class="span9"]//table[2]//tr[${index}]/td[2]/text()'

TEAM_ID_PATTERN = r"\((?P<team_id>[\w]{3})\)"
TEAM_ID_REGEX = re.compile(TEAM_ID_PATTERN)


def parse_pitch_log(scraped_html, game, pitcher_id, url):
    page_content = html.fromstring(scraped_html, base_url=url)
    pitch_log = _initialize_pitch_log(game, pitcher_id, url)
    result = _parse_pitcher_details(page_content, game, pitcher_id)
    if result.failure:
        return Result.Ok(pitch_log)
    pitcher_dict = result.value
    pitch_log.pitcher_name = pitcher_dict["name"]
    pitch_log.pitcher_team_id_bb = pitcher_dict["team_id"]
    pitch_log.opponent_team_id_bb = pitcher_dict["opponent_id"]

    parsed = page_content.xpath(PITCHFX_URL_XPATH)
    if not parsed:
        return Result.Ok(pitch_log)
    rel_url = parsed[0]
    pitch_log.pitchfx_url = Template(T_PITCHFX_URL).substitute(rel_url=rel_url)

    result = _parse_pitch_counts(page_content)
    if result.failure:
        return Result.Ok(pitch_log)
    pitch_log.pitch_count_by_inning = result.value

    total_pitches = 0
    for count in pitch_log.pitch_count_by_inning.values():
        total_pitches += count
    pitch_log.total_pitch_count = int(total_pitches)
    pitch_log.parsed_all_info = True

    return Result.Ok(pitch_log)


def _initialize_pitch_log(game, pitcher_id, url):
    pitch_log = BrooksPitchLog()
    pitch_log.parsed_all_info = False
    pitch_log.pitcher_id_mlb = int(pitcher_id)
    pitch_log.pitch_app_id = f"{game.bbref_game_id}_{pitcher_id}"
    pitch_log.bb_game_id = game.bb_game_id
    pitch_log.bbref_game_id = game.bbref_game_id
    pitch_log.game_date_year = game.game_date_year
    pitch_log.game_date_month = game.game_date_month
    pitch_log.game_date_day = game.game_date_day
    pitch_log.game_time_hour = game.game_time_hour
    pitch_log.game_time_minute = game.game_time_minute
    pitch_log.time_zone_name = game.time_zone_name
    pitch_log.pitch_log_url = url
    pitch_log.pitcher_name = ""
    pitch_log.pitcher_team_id_bb = ""
    pitch_log.opponent_team_id_bb = ""
    pitch_log.pitchfx_url = ""
    pitch_log.pitch_count_by_inning = {}
    pitch_log.total_pitch_count = 0
    return pitch_log


def _parse_pitcher_details(page_content, game, pitcher_id):
    query = Template(T_PITCHER_NAME_XPATH).substitute(id=pitcher_id)
    parsed = page_content.xpath(query)
    if not parsed:
        error = "Failed to parse pitcher name from game log page."
        return Result.Fail(error)

    selected_pitcher = parsed[0]
    indices = [n for n in range(len(selected_pitcher)) if selected_pitcher.find("-", n) == n]
    if not indices or len(indices) < 2:
        error = "Failed to parse pitcher name from game log page."
        return Result.Fail(error)

    indices.reverse()
    name = selected_pitcher[: indices[1]].strip()
    result = _parse_team_ids(game, selected_pitcher)
    if result.failure:
        return result
    id_dict = result.value
    pitcher_dict = dict(name=name, team_id=id_dict["team_id"], opponent_id=id_dict["opponent_id"])
    return Result.Ok(pitcher_dict)


def _parse_team_ids(game, selected_pitcher):
    match = TEAM_ID_REGEX.search(selected_pitcher)
    if not match:
        error = "Unable to parse team ids from game log page."
        return Result.Fail(error)
    match_dict = match.groupdict()
    team_id = match_dict["team_id"]
    if team_id.upper() == game.away_team_id_bb:
        id_dict = dict(team_id=game.away_team_id_bb, opponent_id=game.home_team_id_bb)
    else:
        id_dict = dict(team_id=game.home_team_id_bb, opponent_id=game.away_team_id_bb)
    return Result.Ok(id_dict)


def _parse_pitch_counts(page_content):
    rows = page_content.xpath(PITCH_COUNTS_XPATH)
    if not rows:
        error = "Failed to parse inning-by-inning pitch counts from game log page."
        return Result.Fail(error)
    pitch_totals = {}
    for i in range(2, len(rows)):
        q1 = Template(T_INNING_NUM_XPATH).substitute(index=(i + 1))
        q2 = Template(T_PITCH_COUNT_XPATH).substitute(index=(i + 1))
        inning = page_content.xpath(q1)[0]
        int_str = page_content.xpath(q2)[0]
        count = int(int_str, 10)
        pitch_totals[inning] = count
    return Result.Ok(pitch_totals)
