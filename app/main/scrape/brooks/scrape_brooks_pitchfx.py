"""Scrape brooksbaseball daily dashboard page."""
import time
import uuid
from random import randint
from string import Template

from lxml import html
from tqdm import tqdm

from app.main.constants import PBAR_LEN_DICT
from app.main.scrape.brooks.models.pitchfx_log import BrooksPitchFxLog
from app.main.scrape.brooks.models.pitchfx import BrooksPitchFxData
from app.main.util.decorators import RetryLimitExceededError
from app.main.util.result import Result
from app.main.util.scrape_functions import request_url


DATA_SET = "brooks_pitchfx"
PITCHFX_COLUMN_NAMES = "//tr/th/text()"
PITCHFX_TABLE_ROWS = '//tr[@style="white-space:nowrap"]'
T_PITCHFX_MEASUREMENT = "td[${i}]//text()"
FILTER_NAMES = ["datestamp", "gid", "pitcher_team"]

def scrape_brooks_pitchfx_logs_for_game(pitch_logs_for_game):
    pitchfx_logs_for_game = []
    with tqdm(total=len(pitch_logs_for_game.pitch_logs), unit="pitch_log", leave=False, position=3) as pbar:
        for pitch_log in pitch_logs_for_game.pitch_logs:
            try:
                pbar.set_description(get_pbar_description(pitch_log.pitcher_id_mlb))
                response = request_url(pitch_log.pitchfx_url)
                result = parse_pitchfx_log(response, pitch_log)
                if result.failure:
                    return result
                pitchfx_log = result.value
                pitchfx_logs_for_game.append(pitchfx_log)
                time.sleep(randint(250, 300) / 100.0)
                pbar.update()
            except RetryLimitExceededError as e:
                return Result.Fail(repr(e))
            except Exception as e:
                return Result.Fail(f"Error: {repr(e)}")
    return Result.Ok(pitchfx_logs_for_game)


def get_pbar_description(player_id):
    pre =f"Player ID | {player_id}"
    pad_len = PBAR_LEN_DICT[DATA_SET] - len(pre)
    return f"{pre}{'.'*pad_len}"


def parse_pitchfx_log(response, pitch_log):
    pitchfx_log_dict = {}
    result = parse_pitchfx_table(response, pitch_log)
    if result.failure:
        return result
    pitchfx_data = result.value
    pitchfx_log_dict['pitchfx_log'] = pitchfx_data
    pitchfx_log_dict['pitch_count_by_inning'] = get_pitch_count_by_inning(pitchfx_data)
    pitchfx_log_dict['total_pitch_count'] = len(pitchfx_data)
    pitchfx_log_dict['pitcher_name'] = pitch_log.pitcher_name
    pitchfx_log_dict['pitcher_id_mlb'] = pitch_log.pitcher_id_mlb
    pitchfx_log_dict['pitch_app_id'] = pitch_log.pitch_app_id
    pitchfx_log_dict['pitcher_team_id_bb'] = pitch_log.pitcher_team_id_bb
    pitchfx_log_dict['opponent_team_id_bb'] = pitch_log.opponent_team_id_bb
    pitchfx_log_dict['bb_game_id'] = pitch_log.bb_game_id
    pitchfx_log_dict['bbref_game_id'] = pitch_log.bbref_game_id
    pitchfx_log_dict['pitchfx_url'] = pitch_log.pitchfx_url
    pitchfx_log = BrooksPitchFxLog(**pitchfx_log_dict)
    return Result.Ok(pitchfx_log)


def parse_pitchfx_table(response, pitch_log):
    pitchfx_data = []
    column_names = response.xpath(PITCHFX_COLUMN_NAMES)
    table_rows = response.xpath(PITCHFX_TABLE_ROWS)
    for i, row in enumerate(table_rows):
        result = parse_pitchfx_data(column_names, row, i, pitch_log)
        if result.failure:
            return result
        pitchfx = result.value
        pitchfx_data.append(pitchfx)
    return Result.Ok(pitchfx_data)


def parse_pitchfx_data(column_names, table_row, row_num, pitch_log):
    pitchfx_dict = {}
    for i, name in enumerate(column_names):
        if name.lower() in FILTER_NAMES:
            continue
        query = Template(T_PITCHFX_MEASUREMENT).substitute(i=(i + 1))
        results = table_row.xpath(query)
        if not results:
            if name == "zone_location":
                pitchfx_dict['zone_location'] = 99
                continue
            error = (
                f"Error occurred attempting to parse '{name}' (column #{i}) from pitchfx table:\n"
                f"Game ID.......: {pitch_log.bbref_game_id}\n"
                f"Pitcher.......: {pitch_log.pitcher_name} ({pitch_log.pitcher_id_mlb})\n"
                f"Row Number....: {row_num}\n"
                f"PitchFX URL...: {pitch_log.pitchfx_url}\n"
                f"Partial Data..: {pitchfx_dict}")
            return Result.Fail(error)
        pitchfx_dict[name.lower()] = results[0]
    pitchfx_dict['pitcher_name'] = pitch_log.pitcher_name
    pitchfx_dict['pitch_app_id'] = pitch_log.pitch_app_id
    pitchfx_dict['pitcher_team_id_bb'] = pitch_log.pitcher_team_id_bb
    pitchfx_dict['opponent_team_id_bb'] = pitch_log.opponent_team_id_bb
    pitchfx_dict['bb_game_id'] = pitch_log.bb_game_id
    pitchfx_dict['bbref_game_id'] = pitch_log.bbref_game_id
    if not pitchfx_dict['play_guid']:
        pitchfx_dict['play_guid'] = str(uuid.uuid4())
    pitchfx = BrooksPitchFxData(**pitchfx_dict)
    return Result.Ok(pitchfx)


def get_pitch_count_by_inning(pitchfx_data):
    innings = sorted(list(set([pitchfx.inning for pitchfx in pitchfx_data])))
    pitch_count_dict = {inning:0 for inning in innings}
    for pitchfx in pitchfx_data:
        pitch_count_dict[pitchfx.inning] += 1
    return pitch_count_dict
