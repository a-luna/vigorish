"""Scrape MLB player data"""
import dataclasses
from datetime import datetime, date
from json.decoder import JSONDecodeError
from pathlib import Path

import requests
from dataclass_csv import DataclassReader, accept_whitespaces

from app.main.models.player import Player
from app.main.util.result import Result
from app.main.util.string_functions import fuzzy_match


MAIN_FOLDER = Path(__file__).resolve().parent.parent.parent
PEOPLE_CSV = MAIN_FOLDER / "setup/csv/People.csv"
MLB_PLAYER_SEARCH_URL = "http://lookup-service-prod.mlb.com/json/named.search_player_all.bam"
MLB_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"

@accept_whitespaces
@dataclasses.dataclass
class PlayerID():
    retroID: str = dataclasses.field(default=None)
    bbrefID: str = dataclasses.field(default=None)


def scrape_mlb_player_data(session, name, bbref_id):
    split = name.split()
    if not split or len(split) <= 1:
        return Result.Fail(f"Name was not in an expected format: {name}")
    name_part = ("%20".join(split[1:])).upper()
    search_url = f"{MLB_PLAYER_SEARCH_URL}?sport_code='mlb'&name_part='{name_part}%25'&active_sw='Y'"
    response = requests.get(search_url)
    result = parse_search_results(response, name, name_part, search_url)
    if result.failure:
        return result
    mlb_player_info = result.value
    player_dict = parse_player_data(mlb_player_info, bbref_id)
    new_player = Player(**player_dict)
    session.add(new_player)
    session.commit()
    return Result.Ok(new_player)


def parse_search_results(response, name, name_part, search_url):
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
        mlb_player_info = find_best_match(name, player_list)
    else:
        mlb_player_info = resp_json["search_player_all"]["queryResults"]["row"]
    return Result.Ok(mlb_player_info)


def find_best_match(name, player_list):
    player_name_dict = {
        player["name_display_first_last"]:player["player_id"]
        for player in player_list
    }
    player_info_dict = {player["player_id"]:player for player in player_list}
    match_results = fuzzy_match(name, player_name_dict.keys())
    best_match = match_results["best_match"]
    return player_info_dict[player_name_dict[best_match]]


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
        player_retro_id = [id_map.retroID for id_map in list(people_csv_reader) if id_map.bbrefID == bbref_id]
    return player_retro_id[0] if player_retro_id else ""
