"""Scrape MLB player data"""
import dataclasses
from datetime import datetime, date, timedelta
from json.decoder import JSONDecodeError
from pathlib import Path

import requests
from dataclass_csv import DataclassReader, accept_whitespaces

from vigorish.config.database import Player
from vigorish.util.result import Result
from vigorish.util.string_helpers import fuzzy_match


APP_FOLDER = Path(__file__).parent.parent.parent
PEOPLE_CSV = APP_FOLDER.joinpath("setup/csv/People.csv")
MLB_PLAYER_SEARCH_URL = "http://lookup-service-prod.mlb.com/json/named.search_player_all.bam"
MLB_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


@accept_whitespaces
@dataclasses.dataclass
class PlayerID:
    retroID: str = dataclasses.field(default=None)
    bbrefID: str = dataclasses.field(default=None)


def scrape_mlb_player_info(db_session, name, bbref_id, game_date):
    split = name.split()
    if not split or len(split) <= 1:
        return Result.Fail(f"Name was not in an expected format: {name}")
    name_part = ("%20".join(split[1:])).upper()
    search_url = (
        f"{MLB_PLAYER_SEARCH_URL}?sport_code='mlb'&name_part='{name_part}%25'&active_sw='Y'"
    )
    response = requests.get(search_url)
    result = parse_search_results(response, name, name_part, search_url, game_date)
    if result.failure:
        return result
    mlb_player_info = result.value
    player_dict = parse_player_data(mlb_player_info, bbref_id)
    new_player = Player(**player_dict)
    db_session.add(new_player)
    db_session.commit()
    return Result.Ok(new_player)


def parse_search_results(response, name, name_part, search_url, game_date):
    try:
        resp_json = response.json()
    except JSONDecodeError:
        search_dict = {
            "player_name": name,
            "name_part": name_part,
            "search_url": search_url,
        }
        return Result.Fail(f"Failed to retrieve MLB player data: {search_dict}")
    num_results_str = resp_json["search_player_all"]["queryResults"]["totalSize"]

    try:
        num_results = int(num_results_str)
    except ValueError:
        error = f"Failed to parse number of results from search response: {resp_json}"
        return Result.Fail(error)

    if num_results > 1:
        player_list = resp_json["search_player_all"]["queryResults"]["row"]
        mlb_player_info = find_best_match(name, player_list, game_date)
    else:
        mlb_player_info = resp_json["search_player_all"]["queryResults"]["row"]
    return Result.Ok(mlb_player_info)


def find_best_match(name, player_list, game_date):
    player_id_name_map = {
        player["player_id"]: player["name_display_first_last"] for player in player_list
    }
    player_info_dict = {player["player_id"]: player for player in player_list}
    match_results = fuzzy_match(name, player_id_name_map)
    if len(match_results) == 1:
        return player_info_dict[match_results[0]["result"]]
    return compare_mlb_debut(match_results, player_info_dict, game_date)


def compare_mlb_debut(possible_matches, player_info_dict, game_date):
    info_dict_list = [
        player_info_dict[possible_match["result"]] for possible_match in possible_matches
    ]
    for player_info in info_dict_list:
        if not player_info["pro_debut_date"]:
            player_info["since_debut"] = timedelta.max.days
            continue
        player_mlb_debut = datetime.strptime(player_info["pro_debut_date"], MLB_DATE_FORMAT)
        player_info["since_debut"] = abs((game_date - player_mlb_debut.date()).days)
    info_dict_list.sort(key=lambda x: x["since_debut"])
    return info_dict_list[0]


def parse_player_data(player_info, bbref_id):
    try:
        feet = int(player_info["height_feet"])
        inches = int(player_info["height_inches"])
        height_total_inches = (feet * 12) + inches
    except ValueError:
        height_total_inches = 0

    try:
        debut_str = player_info["pro_debut_date"]
        debut = datetime.strptime(debut_str, MLB_DATE_FORMAT)
        debut = debut.date()
        birth_date_str = player_info["birth_date"]
        birth_date = datetime.strptime(birth_date_str, MLB_DATE_FORMAT)
        birth_date = birth_date.date()
    except ValueError:
        debut = date.min
        birth_date = date.min

    player_dict = {
        "name_first": player_info["name_first"],
        "name_last": player_info["name_last"],
        "name_given": player_info["name_first"],
        "bats": player_info["bats"],
        "throws": player_info["throws"],
        "weight": player_info["weight"],
        "height": height_total_inches,
        "debut": debut,
        "birth_year": birth_date.year,
        "birth_month": birth_date.month,
        "birth_day": birth_date.day,
        "birth_country": player_info["birth_country"],
        "birth_state": player_info["birth_state"],
        "birth_city": player_info["birth_city"],
        "bbref_id": bbref_id,
        "retro_id": retrieve_retro_id(bbref_id),
        "mlb_id": player_info["player_id"],
        "missing_mlb_id": False,
    }
    return player_dict


def retrieve_retro_id(bbref_id):
    with open(PEOPLE_CSV) as people_csv:
        people_csv_reader = DataclassReader(people_csv, PlayerID)
        player_retro_id = [
            id_map.retroID for id_map in list(people_csv_reader) if id_map.bbrefID == bbref_id
        ]
    return player_retro_id[0] if player_retro_id else ""
