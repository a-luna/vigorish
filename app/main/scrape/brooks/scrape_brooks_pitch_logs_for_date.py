"""Scrape brooksbaseball daily dashboard page."""
import re
import time
from random import randint
from string import Template

from lxml import html
from tqdm import tqdm

from app.main.constants import PBAR_LEN_DICT
from app.main.scrape.brooks.models.pitch_logs_for_game import BrooksPitchLogsForGame
from app.main.scrape.brooks.models.pitch_log import BrooksPitchLog
from app.main.util.decorators import RetryLimitExceededError
from app.main.util.result import Result
from app.main.util.scrape_functions import request_url


DATA_SET = "brooks_pitch_logs"

PITCHFX_URL_XPATH = '//a[text()="Get Expanded Tabled Data"]/@href'
PITCH_COUNTS_XPATH = '//div[@class="span9"]//table[2]//tr'

T_PITCHFX_URL = "http://www.brooksbaseball.net/pfxVB/${rel_url}"
T_PITCHER_NAME_XPATH = '//option[@value="${id}"]/text()'
T_INNING_NUM_XPATH = '//div[@class="span9"]//table[2]//tr[${index}]/td[1]/text()'
T_PITCH_COUNT_XPATH = '//div[@class="span9"]//table[2]//tr[${index}]/td[2]/text()'

TEAM_ID_PATTERN = r"\((?P<team_id>[\w]{3})\)"
TEAM_ID_REGEX = re.compile(TEAM_ID_PATTERN)


def scrape_brooks_pitch_logs_for_date(pitch_logs_for_date, games_for_date):
    scraped_games = []
    scraped_pitch_logs_dict = {g.bb_game_id:g for g in pitch_logs_for_date}
    with tqdm(total=len(games_for_date.games), unit="game", leave=False, position=2) as pbar:
        for game in games_for_date.games:
            if game.might_be_postponed:
                continue
            pbar.set_description(get_pbar_game_description(game.bbref_game_id))
            scraped_pitch_logs_for_game = scraped_pitch_logs_dict.get(game.bb_game_id, None)
            result = parse_pitch_logs_for_game(scraped_pitch_logs_for_game, game)
            if result.failure:
                return result
            brooks_pitch_logs = result.value
            scraped_games.append(brooks_pitch_logs)
            pbar.update()
    return Result.Ok(scraped_games)


def get_pbar_game_description(game_id):
    pre =f"Game ID   | {game_id}"
    pad_len = PBAR_LEN_DICT[DATA_SET] - len(pre)
    return f"{pre}{'.'*pad_len}"


def parse_pitch_logs_for_game(scraped_pitch_logs_for_game, game):
    pitch_logs_for_game = BrooksPitchLogsForGame()
    pitch_logs_for_game.bb_game_id = game.bb_game_id
    pitch_logs_for_game.bbref_game_id = game.bbref_game_id
    pitch_logs_for_game.pitch_log_count = game.pitcher_appearance_count
    pitch_app_dict = game.pitcher_appearance_dict

    scraped_pitch_logs = []
    with tqdm(total=len(pitch_app_dict), unit="pitch_log",  leave=False, position=3) as pbar:
        for pitcher_id, url in pitch_app_dict.items():
            try:
                pbar.set_description(get_pbar_pitch_log_description(pitcher_id))
                already_scraped = False
                if hasattr(scraped_pitch_logs_for_game, "pitch_logs"):
                    already_scraped = any(
                        pitch_log.pitcher_id_mlb == pitcher_id and pitch_log.parsed_all_info
                        for pitch_log in scraped_pitch_logs_for_game.pitch_logs
                    )
                if already_scraped:
                    time.sleep(randint(50, 75) / 100.0)
                    pbar.update()
                pbar.set_description(get_pbar_description_requesting(pitcher_id))
                response = request_url(url)
                result = parse_pitch_log(response, game, pitcher_id, url)
                if result.failure:
                    return result
                brooks_pitch_log = result.value
                scraped_pitch_logs.append(brooks_pitch_log)
                time.sleep(randint(250, 300) / 100.0)
                pbar.update()
            except RetryLimitExceededError as e:
                return Result.Fail(repr(e))
            except Exception as e:
                return Result.Fail(f"Error: {repr(e)}")

    pitch_logs_for_game.pitch_logs = scraped_pitch_logs
    return Result.Ok(pitch_logs_for_game)


def get_pbar_pitch_log_description(player_id):
    pre =f"Player ID | {player_id}"
    pad_len = PBAR_LEN_DICT[DATA_SET] - len(pre)
    return f"{pre}{'.'*pad_len}"


def get_pbar_description_requesting(player_id):
    pre =f"FETCHING  | {player_id}"
    pad_len = PBAR_LEN_DICT[DATA_SET] - len(pre)
    return f"{pre}{'.'*pad_len}"


def parse_pitch_log(response, game, pitcher_id, url):
    pitch_log = _initialize_pitch_log(game, pitcher_id, url)
    result = _parse_pitcher_details(response, game, pitcher_id)
    if result.failure:
        return Result.Ok(pitch_log)
    pitcher_dict = result.value
    pitch_log.pitcher_name = pitcher_dict["name"]
    pitch_log.pitcher_team_id_bb = pitcher_dict["team_id"]
    pitch_log.opponent_team_id_bb = pitcher_dict["opponent_id"]

    parsed = response.xpath(PITCHFX_URL_XPATH)
    if not parsed:
        return Result.Ok(pitch_log)
    rel_url = parsed[0]
    pitch_log.pitchfx_url = Template(T_PITCHFX_URL).substitute(rel_url=rel_url)

    result = _parse_pitch_counts(response)
    if result.failure:
        return Result.Ok(pitch_log)
    pitch_log.pitch_count_by_inning = result.value

    total_pitches = 0
    for count in pitch_log.pitch_count_by_inning.values():
        total_pitches += count
    pitch_log.total_pitch_count = total_pitches
    pitch_log.parsed_all_info = True

    return Result.Ok(pitch_log)


def _initialize_pitch_log(game, pitcher_id, url):
    pitch_log = BrooksPitchLog()
    pitch_log.parsed_all_info = False
    pitch_log.pitcher_id_mlb = pitcher_id
    pitch_log.pitch_app_id = f"{game.bbref_game_id}_{pitcher_id}"
    pitch_log.bb_game_id = game.bb_game_id
    pitch_log.bbref_game_id = game.bbref_game_id
    pitch_log.pitch_log_url = url
    pitch_log.pitcher_name = ""
    pitch_log.pitcher_team_id_bb = ""
    pitch_log.opponent_team_id_bb = ""
    pitch_log.pitchfx_url = ""
    pitch_log.pitch_count_by_inning = {}
    pitch_log.total_pitch_count = 0
    return pitch_log


def _parse_pitcher_details(response, game, pitcher_id):
    query = Template(T_PITCHER_NAME_XPATH).substitute(id=pitcher_id)
    parsed = response.xpath(query)
    if not parsed:
        error = "Failed to parse pitcher name from game log page."
        return Result.Fail(error)

    selected_pitcher = parsed[0]
    indices = [
        n for n in range(len(selected_pitcher)) if selected_pitcher.find("-", n) == n
    ]
    if not indices or len(indices) < 2:
        error = "Failed to parse pitcher name from game log page."
        return Result.Fail(error)

    indices.reverse()
    name = selected_pitcher[: indices[1]].strip()
    result = _parse_team_ids(game, selected_pitcher)
    if result.failure:
        return result
    id_dict = result.value
    pitcher_dict = dict(
        name=name, team_id=id_dict["team_id"], opponent_id=id_dict["opponent_id"]
    )
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


def _parse_pitch_counts(response):
    rows = response.xpath(PITCH_COUNTS_XPATH)
    if not rows:
        error = "Failed to parse inning-by-inning pitch counts from game log page."
        return Result.Fail(error)
    pitch_totals = {}
    for i in range(2, len(rows)):
        q1 = Template(T_INNING_NUM_XPATH).substitute(index=(i + 1))
        q2 = Template(T_PITCH_COUNT_XPATH).substitute(index=(i + 1))
        inning = response.xpath(q1)[0]
        int_str = response.xpath(q2)[0]
        count = int(int_str, 10)
        pitch_totals[inning] = count
    return Result.Ok(pitch_totals)
