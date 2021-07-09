"""Scrape brooksbaseball daily dashboard page."""
import uuid
from collections import defaultdict, OrderedDict
from string import Template

from lxml import html

from vigorish.constants import (
    PITCH_DES_DID_SWING,
    PITCH_DES_MADE_CONTACT,
    PITCH_DES_SWINGING_STRIKE,
    ZONE_LOCATIONS_INSIDE_SZ,
)
from vigorish.scrape.brooks_pitchfx.models.pitchfx import BrooksPitchFxData
from vigorish.scrape.brooks_pitchfx.models.pitchfx_log import BrooksPitchFxLog
from vigorish.util.dt_format_strings import DT_AWARE
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
    pitchfx_log_dict["game_date_year"] = pitch_log.game_date_year
    pitchfx_log_dict["game_date_month"] = pitch_log.game_date_month
    pitchfx_log_dict["game_date_day"] = pitch_log.game_date_day
    pitchfx_log_dict["game_time_hour"] = pitch_log.game_time_hour
    pitchfx_log_dict["game_time_minute"] = pitch_log.game_time_minute
    pitchfx_log_dict["time_zone_name"] = pitch_log.time_zone_name
    pitchfx_log_dict["pitchfx_url"] = pitch_log.pitchfx_url
    pitchfx_log = BrooksPitchFxLog(**pitchfx_log_dict)
    return Result.Ok(pitchfx_log)


def get_pitch_count_by_inning(pfx_data):
    unordered = defaultdict(int)
    for pfx in pfx_data:
        unordered[pfx.inning] += 1
    pitch_count_by_inning = OrderedDict()
    for k in sorted(unordered.keys()):
        pitch_count_by_inning[k] = unordered[k]
    return pitch_count_by_inning


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
            if name == "des":
                pitchfx_dict["des"] = "missing_des"
                continue
            elif name == "pdes":
                pitchfx_dict["pdes"] = "missing_pdes"
                continue
            elif name == "play_guid":
                pitchfx_dict["play_guid"] = str(uuid.uuid4())
                continue
            elif name == "zone_location":
                pitchfx_dict["zone_location"] = 99
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
        pitchfx_dict[name.lower()] = convert_type_for_dataclass(name.lower(), results[0])
    pitchfx_dict["pitcher_name"] = pitch_log.pitcher_name
    pitchfx_dict["pitch_app_id"] = pitch_log.pitch_app_id
    pitchfx_dict["pitcher_team_id_bb"] = pitch_log.pitcher_team_id_bb
    pitchfx_dict["opponent_team_id_bb"] = pitch_log.opponent_team_id_bb
    pitchfx_dict["bb_game_id"] = pitch_log.bb_game_id
    pitchfx_dict["bbref_game_id"] = pitch_log.bbref_game_id
    pitchfx_dict = remove_invalid_dict_values(pitchfx_dict)
    pitchfx = BrooksPitchFxData(**pitchfx_dict)

    game_start_time = pitch_log.game_start_time
    pitchfx.game_start_time_str = game_start_time.strftime(DT_AWARE) if game_start_time else ""
    pitchfx.has_zone_location = pitchfx.zone_location != 99
    pitchfx.batter_did_swing = pitchfx_dict["pdes"] in PITCH_DES_DID_SWING
    pitchfx.batter_made_contact = pitchfx_dict["pdes"] in PITCH_DES_MADE_CONTACT
    pitchfx.called_strike = "Called Strike" in pitchfx_dict["pdes"]
    pitchfx.swinging_strike = pitchfx_dict["pdes"] in PITCH_DES_SWINGING_STRIKE
    if pitchfx.has_zone_location:
        pitchfx.inside_strike_zone = pitchfx.zone_location in ZONE_LOCATIONS_INSIDE_SZ
        pitchfx.outside_strike_zone = not pitchfx.inside_strike_zone
        pitchfx.swing_inside_zone = pitchfx.batter_did_swing and pitchfx.inside_strike_zone
        pitchfx.swing_outside_zone = pitchfx.batter_did_swing and pitchfx.outside_strike_zone
        pitchfx.contact_inside_zone = pitchfx.batter_made_contact and pitchfx.inside_strike_zone
        pitchfx.contact_outside_zone = pitchfx.batter_made_contact and pitchfx.outside_strike_zone
    return Result.Ok(pitchfx)


def convert_type_for_dataclass(name, value):
    dataclass_field = BrooksPitchFxData.__dataclass_fields__.get(name)
    field_type = dataclass_field.type if dataclass_field else str
    return (
        True
        if field_type is bool and value
        else False
        if field_type is bool and not value
        else None
        if not value
        else int(value)
        if field_type is int and not isinstance(value, int)
        else float(value)
        if field_type is float and not isinstance(value, float)
        else value
    )


def fix_missing_des(pfx_data, pitch_log):
    missing_des = any(pfx.des == "missing_des" for pfx in pfx_data)
    if not missing_des:
        return Result.Ok(pfx_data)
    fix_ab_ids = {pfx.ab_id for pfx in pfx_data if pfx.des == "missing_des"}
    for ab_id in list(fix_ab_ids):
        missing_des = [pfx for pfx in pfx_data if this_ab_and_missing_des(pfx, ab_id)]
        valid_des = list({pfx.des for pfx in pfx_data if this_ab_and_has_des(pfx, ab_id)})
        if not valid_des:
            continue
        if len(valid_des) == 1:
            for pfx in missing_des:
                pfx.des = valid_des[0]
            continue
        error = (
            f"Unable to fix missing description for pitchfx ab_id={ab_id}\n"
            f"Game ID.......: {pitch_log.bbref_game_id}\n"
            f"Pitcher.......: {pitch_log.pitcher_name} ({pitch_log.pitcher_id_mlb})\n"
            f"PitchFX URL...: {pitch_log.pitchfx_url}\n"
            f"des values....: {valid_des}"
        )
        return Result.Fail(error)
    return Result.Ok(pfx_data)


def this_ab_and_missing_des(pfx, ab_id):
    return pfx.ab_id == ab_id and pfx.des == "missing_des"


def this_ab_and_has_des(pfx, ab_id):
    return pfx.ab_id == ab_id and pfx.des != "missing_des"


def remove_invalid_dict_values(pitchfx_dict):
    pitchfx_dict.pop("pfx_xdatafile", "")
    pitchfx_dict.pop("pfx_zdatafile", "")
    pitchfx_dict.pop("pitch_con", "")
    pitchfx_dict.pop("gid", "")
    pitchfx_dict.pop("norm_ht", "")
    pitchfx_dict.pop("spin", "")
    pitchfx_dict.pop("tstart", "")
    pitchfx_dict.pop("vystart", "")
    pitchfx_dict.pop("ftime", "")
    pitchfx_dict.pop("uncorrected_pfx_x", "")
    pitchfx_dict.pop("uncorrected_pfx_z", "")
    pitchfx_dict.pop("pxold", "")
    pitchfx_dict.pop("pzold", "")
    pitchfx_dict.pop("tm_spin", "")
    pitchfx_dict.pop("sb", "")
    pitchfx_dict.pop("tm_spin", "")
    pitchfx_dict.pop("sb", "")
    pitchfx_dict["pitcher_team_id_bb"] = pitchfx_dict.pop("pitcher_team", "")
    return pitchfx_dict
