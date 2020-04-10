"""Scrape brooksbaseball daily dashboard page."""
import uuid
from string import Template

from lxml import html

from vigorish.scrape.brooks_pitchfx.models.pitchfx_log import BrooksPitchFxLog
from vigorish.scrape.brooks_pitchfx.models.pitchfx import BrooksPitchFxData
from vigorish.util.result import Result

PITCHFX_COLUMN_NAMES = "//tr/th/text()"
PITCHFX_TABLE_ROWS = '//tr[@style="white-space:nowrap"]'
T_PITCHFX_MEASUREMENT = "td[${i}]//text()"
FILTER_NAMES = ["datestamp", "gid", "pitcher_team"]


def parse_pitchfx_log(scraped_html, pitch_log):
    page_content = html.fromstring(scraped_html, base_url=pitch_log.pitchfx_url)
    pitchfx_log_dict = {}
    result = parse_pitchfx_table(page_content, pitch_log)
    if result.failure:
        return result
    pitchfx_data = result.value
    pitchfx_log_dict["pitchfx_log"] = pitchfx_data
    pitchfx_log_dict["pitch_count_by_inning"] = get_pitch_count_by_inning(pitchfx_data)
    pitchfx_log_dict["total_pitch_count"] = len(pitchfx_data)
    pitchfx_log_dict["pitcher_name"] = pitch_log.pitcher_name
    pitchfx_log_dict["pitcher_id_mlb"] = pitch_log.pitcher_id_mlb
    pitchfx_log_dict["pitch_app_id"] = pitch_log.pitch_app_id
    pitchfx_log_dict["pitcher_team_id_bb"] = pitch_log.pitcher_team_id_bb
    pitchfx_log_dict["opponent_team_id_bb"] = pitch_log.opponent_team_id_bb
    pitchfx_log_dict["bb_game_id"] = pitch_log.bb_game_id
    pitchfx_log_dict["bbref_game_id"] = pitch_log.bbref_game_id
    pitchfx_log_dict["pitchfx_url"] = pitch_log.pitchfx_url
    pitchfx_log = BrooksPitchFxLog(**pitchfx_log_dict)
    return Result.Ok(pitchfx_log)


def parse_pitchfx_table(page_content, pitch_log):
    pitchfx_data = []
    column_names = page_content.xpath(PITCHFX_COLUMN_NAMES)
    table_rows = page_content.xpath(PITCHFX_TABLE_ROWS)
    for i, row in enumerate(table_rows):
        result = parse_pitchfx_data(column_names, row, i, pitch_log)
        if result.failure:
            return result
        pitchfx = result.value
        pitchfx_data.append(pitchfx)
    return fix_missing_des(pitchfx_data, pitch_log)


def parse_pitchfx_data(column_names, table_row, row_num, pitch_log):
    pitchfx_dict = {}
    for i, name in enumerate(column_names):
        if name.lower() in FILTER_NAMES:
            continue
        query = Template(T_PITCHFX_MEASUREMENT).substitute(i=(i + 1))
        results = table_row.xpath(query)
        if not results:
            if name == "zone_location":
                pitchfx_dict["zone_location"] = 99
                continue
            if name == "des":
                pitchfx_dict["des"] = "missing_des"
                continue
            if name == "pdes":
                pitchfx_dict["pdes"] = "missing_pdes"
                continue
            if name == "play_guid":
                pitchfx_dict["play_guid"] = str(uuid.uuid4())
                continue
            error = (
                f"Error occurred attempting to parse '{name}' (column #{i}) from pitchfx table:\n"
                f"Game ID.......: {pitch_log.bbref_game_id}\n"
                f"Pitcher.......: {pitch_log.pitcher_name} ({pitch_log.pitcher_id_mlb})\n"
                f"Row Number....: {row_num}\n"
                f"PitchFX URL...: {pitch_log.pitchfx_url}\n"
                f"Partial Data..: {pitchfx_dict}"
            )
            return Result.Fail(error)
        pitchfx_dict[name.lower()] = results[0]
    pitchfx_dict["pitcher_name"] = pitch_log.pitcher_name
    pitchfx_dict["pitch_app_id"] = pitch_log.pitch_app_id
    pitchfx_dict["pitcher_team_id_bb"] = pitch_log.pitcher_team_id_bb
    pitchfx_dict["opponent_team_id_bb"] = pitch_log.opponent_team_id_bb
    pitchfx_dict["bb_game_id"] = pitch_log.bb_game_id
    pitchfx_dict["bbref_game_id"] = pitch_log.bbref_game_id
    pitchfx_dict["table_row_number"] = row_num
    pitchfx = BrooksPitchFxData(**pitchfx_dict)
    return Result.Ok(pitchfx)


def fix_missing_des(pitchfx_data, pitch_log):
    missing_des = any(pfx.des == "missing_des" for pfx in pitchfx_data)
    if not missing_des:
        return Result.Ok(pitchfx_data)
    fix_ab_ids = list(set([pfx.ab_id for pfx in pitchfx_data if pfx.des == "missing_des"]))
    for ab_id in fix_ab_ids:
        missing_des_this_ab = [
            pfx for pfx in pitchfx_data if pfx.ab_id == ab_id and pfx.des == "missing_des"
        ]
        valid_des_this_ab = list(
            set(
                [
                    pfx.des
                    for pfx in pitchfx_data
                    if pfx.ab_id == ab_id and not pfx.des == "missing_des"
                ]
            )
        )
        if len(valid_des_this_ab) == 0:
            continue
        if len(valid_des_this_ab) == 1:
            for pfx in missing_des_this_ab:
                pfx.des = valid_des_this_ab[0]
            continue
        error = (
            f"Unable to fix missing description for pitchfx ab_id={ab_id}\n"
            f"Game ID.......: {pitch_log.bbref_game_id}\n"
            f"Pitcher.......: {pitch_log.pitcher_name} ({pitch_log.pitcher_id_mlb})\n"
            f"PitchFX URL...: {pitch_log.pitchfx_url}\n"
            f"des values....: {valid_des_this_ab}"
        )
        return Result.Fail(error)
    return Result.Ok(pitchfx_data)


def get_pitch_count_by_inning(pitchfx_data):
    innings = sorted(list(set([pitchfx.inning for pitchfx in pitchfx_data])))
    pitch_count_dict = {inning: 0 for inning in innings}
    for pitchfx in pitchfx_data:
        pitch_count_dict[pitchfx.inning] += 1
    return pitch_count_dict
