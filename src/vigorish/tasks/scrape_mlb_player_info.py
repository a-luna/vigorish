"""Scrape MLB player data"""
from datetime import date, datetime, timedelta
from json.decoder import JSONDecodeError

from events import Events
from sqlalchemy.exc import DBAPIError, SQLAlchemyError

import vigorish.database as db
from vigorish.tasks.base import Task
from vigorish.util.request_url import request_url_with_retries
from vigorish.util.result import Result
from vigorish.util.string_helpers import fuzzy_match

MLB_PLAYER_SEARCH_URL = "http://lookup-service-prod.mlb.com/json/named.search_player_all.bam"
MLB_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


class ScrapeMlbPlayerInfoTask(Task):
    def __init__(self, app):
        super().__init__(app)
        self.events = Events(
            (
                "error_occurred",
                "scrape_player_info_start",
                "scrape_player_info_complete",
            )
        )

    def execute(self, name, bbref_id, game_date):
        return (
            self.get_search_url(name)
            .on_success(request_url_with_retries)
            .on_success(self.decode_json_response)
            .on_success(self.get_player_data, name, game_date)
            .on_success(self.parse_player_data, bbref_id)
            .on_success(self.add_player_to_database)
        )

    def get_search_url(self, name):
        self.events.scrape_player_info_start(name)
        split = name.split()
        if not split or len(split) <= 1:  # pragma: no cover
            return Result.Fail(f"Name was not in an expected format: {name}")
        name_part = ("%20".join(split[1:])).upper()
        url = f"{MLB_PLAYER_SEARCH_URL}?sport_code='mlb'&name_part='{name_part}%25'&active_sw='Y'"
        return Result.Ok(url)

    def decode_json_response(self, response):
        query_results = ""
        try:
            resp_json = response.json()
            query_results = resp_json["search_player_all"]["queryResults"]
            num_results = int(query_results["totalSize"])
            return Result.Ok((query_results, num_results))
        except (JSONDecodeError, KeyError) as e:  # pragma: no cover
            error = f"Failed to decode HTTP response as JSON: {repr(e)}\n{e.response.text}"
            return Result.Fail(error)
        except ValueError:  # pragma: no cover
            error = f"Failed to parse number of results from search response: {query_results}"
            return Result.Fail(error)

    def get_player_data(self, results_tuple, name, game_date):
        results = results_tuple[0]
        num_results = results_tuple[1]
        if num_results > 1:
            player_list = results["row"]
            player_data = self.find_best_match(player_list, name, game_date)
        else:
            player_data = results["row"]
        return Result.Ok(player_data)

    def find_best_match(self, player_list, name, game_date):
        player_id_name_map = {player["player_id"]: player["name_display_first_last"] for player in player_list}
        player_info_dict = {player["player_id"]: player for player in player_list}
        possible_matches = fuzzy_match(name, player_id_name_map)
        if len(possible_matches) == 1:
            return player_info_dict[possible_matches[0]["result"]]
        return self.compare_mlb_debut(possible_matches, player_info_dict, game_date)

    def compare_mlb_debut(self, possible_matches, player_info_dict, game_date):
        info_dict_list = [player_info_dict[possible_match["result"]] for possible_match in possible_matches]
        for player_info in info_dict_list:
            if not player_info["pro_debut_date"]:  # pragma: no cover
                player_info["since_debut"] = timedelta.max.days
                continue
            player_mlb_debut = datetime.strptime(player_info["pro_debut_date"], MLB_DATE_FORMAT)
            player_info["since_debut"] = abs((game_date - player_mlb_debut.date()).days)
        info_dict_list.sort(key=lambda x: x["since_debut"])
        return info_dict_list[0]

    def parse_player_data(self, player_data, bbref_id):
        try:
            feet = int(player_data["height_feet"])
            inches = int(player_data["height_inches"])
            height_total_inches = (feet * 12) + inches
        except ValueError:  # pragma: no cover
            height_total_inches = 0

        try:
            debut = datetime.strptime(player_data["pro_debut_date"], MLB_DATE_FORMAT).date()
            birth_date = datetime.strptime(player_data["birth_date"], MLB_DATE_FORMAT).date()
        except ValueError:  # pragma: no cover
            debut = date.min
            birth_date = date.min

        player_dict = {
            "name_first": player_data["name_first"],
            "name_last": player_data["name_last"],
            "name_given": player_data["name_first"],
            "bats": player_data["bats"],
            "throws": player_data["throws"],
            "weight": player_data["weight"],
            "height": height_total_inches,
            "debut": debut,
            "birth_year": birth_date.year,
            "birth_month": birth_date.month,
            "birth_day": birth_date.day,
            "birth_country": player_data["birth_country"],
            "birth_state": player_data["birth_state"],
            "birth_city": player_data["birth_city"],
            "bbref_id": bbref_id,
            "mlb_id": player_data["player_id"],
            "missing_mlb_id": False,
        }
        return Result.Ok(player_dict)

    def add_player_to_database(self, player_dict):
        try:
            new_player = db.Player(**player_dict)
            self.db_session.add(new_player)
            self.db_session.commit()
            self.events.scrape_player_info_complete(new_player)
            return Result.Ok(new_player)
        except (SQLAlchemyError, DBAPIError) as e:  # pragma: no cover
            return Result.Fail(f"Error: {repr(e)}")
