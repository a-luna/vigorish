"""Scrape MLB player data"""
from datetime import date, datetime, timedelta
from json.decoder import JSONDecodeError

from sqlalchemy.exc import DBAPIError, SQLAlchemyError

from vigorish.config.database import Player
from vigorish.util.request_url import request_url_with_retries
from vigorish.util.result import Result
from vigorish.util.string_helpers import fuzzy_match

MLB_PLAYER_SEARCH_URL = "http://lookup-service-prod.mlb.com/json/named.search_player_all.bam"
MLB_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


def scrape_mlb_player_info(db_session, name, bbref_id, game_date):
    return (
        get_search_url(name)
        .on_success(request_url_with_retries)
        .on_success(decode_json_response)
        .on_success(get_player_data, name, game_date)
        .on_success(parse_player_data, bbref_id, db_session)
    )


def get_search_url(name):
    split = name.split()
    if not split or len(split) <= 1:
        return Result.Fail(f"Name was not in an expected format: {name}")
    name_part = ("%20".join(split[1:])).upper()
    url = f"{MLB_PLAYER_SEARCH_URL}?sport_code='mlb'&name_part='{name_part}%25'&active_sw='Y'"
    return Result.Ok(url)


def decode_json_response(response):
    try:
        resp_json = response.json()
        query_results = resp_json["search_player_all"]["queryResults"]
        num_results = int(query_results["totalSize"])
        return Result.Ok((query_results, num_results))
    except (JSONDecodeError, KeyError) as e:
        error = f"Failed to decode HTTP response as JSON: {repr(e)}\n{e.response.text}"
        return Result.Fail(error)
    except ValueError:
        error = f"Failed to parse number of results from search response: {query_results}"
        return Result.Fail(error)


def get_player_data(results_tuple, name, game_date):
    results = results_tuple[0]
    num_results = results_tuple[1]
    if num_results > 1:
        player_list = results["row"]
        player_data = find_best_match(player_list, name, game_date)
    else:
        player_data = results["row"]
    return Result.Ok(player_data)


def find_best_match(player_list, name, game_date):
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


def parse_player_data(player_info, bbref_id, db_session):
    try:
        feet = int(player_info["height_feet"])
        inches = int(player_info["height_inches"])
        height_total_inches = (feet * 12) + inches
    except ValueError:
        height_total_inches = 0

    try:
        debut = datetime.strptime(player_info["pro_debut_date"], MLB_DATE_FORMAT).date()
        birth_date = datetime.strptime(player_info["birth_date"], MLB_DATE_FORMAT).date()
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
        "mlb_id": player_info["player_id"],
        "missing_mlb_id": False,
    }

    try:
        new_player = Player(**player_dict)
        db_session.add(new_player)
        db_session.commit()
        return Result.Ok(new_player)
    except (SQLAlchemyError, DBAPIError) as e:
        return Result.Fail(f"Error: {repr(e)}")
