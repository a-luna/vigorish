"""Scrape MLB player data"""
import re
from datetime import date, datetime, timedelta
from json.decoder import JSONDecodeError

from events import Events
from sqlalchemy.exc import SQLAlchemyError

import vigorish.database as db
from vigorish.tasks.base import Task
from vigorish.util.dt_format_strings import DATE_ONLY
from vigorish.util.request_url import request_url_with_retries
from vigorish.util.result import Result
from vigorish.util.string_helpers import fuzzy_match, remove_accents

MLB_PLAYER_SEARCH_URL = "http://lookup-service-prod.mlb.com/json/named.search_player_all.bam"
MLB_API_PLAYER_URL = "https://statsapi.mlb.com/api/v1/people/"
MLB_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
HEIGHT_REGEX = re.compile(r"(?P<feet>\d)' (?P<inches>\d{1,2})\"")


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

    def find_by_mlb_id(self, mlb_id, bbref_id, debut_limit=None):
        url = f"{MLB_API_PLAYER_URL}{mlb_id}"
        result = request_url_with_retries(url)
        if result.failure:
            return result
        response = result.value
        resp_json = response.json()
        if "people" not in resp_json or len(resp_json["people"]) != 1:
            return Result.Fail("Response JSON was not in the expected format")
        result = self.parse_player_data_v2(resp_json["people"][0], bbref_id, debut_limit)
        if result.failure:
            if "Player debuted before the debut limit" in result.error:
                return Result.Ok({})
            return result
        player_dict = result.value
        return self.add_player_to_database_v2(player_dict)

    def parse_player_data_v2(self, player_data, bbref_id, debut_limit=None):
        try:
            debut = datetime.strptime(player_data.get("mlbDebutDate", ""), DATE_ONLY).date()
        except ValueError:  # pragma: no cover
            debut = date.min

        if debut_limit and debut.year < debut_limit:
            return Result.Fail("Player debuted before the debut limit")

        try:
            birth_date = datetime.strptime(player_data.get("birthDate", ""), DATE_ONLY).date()
        except ValueError:  # pragma: no cover
            birth_date = date.min

        match = HEIGHT_REGEX.search(player_data.get("height", r"0' 0\""))
        if not match:
            return Result.Fail("Response JSON was not in the expected format")
        groups = match.groupdict()
        height_total_inches = int(groups["feet"]) * 12 + int(groups["inches"])

        name_given = (
            f'{player_data.get("firstName", "")} {player_data["middleName"]}'
            if "middleName" in player_data
            else player_data.get("firstName", "")
        )
        first_name = player_data.get("useName", "") if "useName" in player_data else player_data.get("firstName", "")
        bats = player_data.get("batSide", {})
        throws = player_data.get("pitchHand", {})

        player_dict = {
            "name_first": first_name,
            "name_last": player_data.get("lastName", ""),
            "name_given": name_given,
            "bats": bats.get("code", ""),
            "throws": throws.get("code", ""),
            "weight": player_data.get("weight"),
            "height": height_total_inches,
            "debut": debut,
            "birth_year": birth_date.year,
            "birth_month": birth_date.month,
            "birth_day": birth_date.day,
            "birth_country": player_data.get("birthCountry", ""),
            "birth_state": player_data.get("birthStateProvince", ""),
            "birth_city": player_data.get("birthCity", ""),
            "bbref_id": bbref_id,
            "mlb_id": player_data["id"],
            "add_to_db_backup": True,
        }
        return Result.Ok(player_dict)

    def add_player_to_database_v2(self, player_dict):
        try:
            new_player = db.Player(**player_dict)
            self.db_session.add(new_player)
            self.db_session.commit()
            self.events.scrape_player_info_complete(new_player)
            return Result.Ok(new_player)
        except SQLAlchemyError as e:  # pragma: no cover
            return Result.Fail(f"Error: {repr(e)}")

    def execute(self, name, bbref_id, game_date):
        name = remove_accents(name)
        self.name = name
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
            if not num_results:
                result = self.try_alternate_url().on_success(request_url_with_retries)
                if result.failure:
                    return result
                response = result.value
                resp_json = response.json()
                query_results = resp_json["search_player_all"]["queryResults"]
                num_results = int(query_results["totalSize"])
            if not num_results:
                return Result.Fail(f"Failed to retrieve any results for player name: {self.name} (Tried 2 URLs)")
            return Result.Ok((query_results, num_results))
        except (JSONDecodeError, KeyError) as e:
            error = f"Failed to decode HTTP response as JSON: {repr(e)}\n{e.response.text}"
            return Result.Fail(error)
        except ValueError:  # pragma: no cover
            error = f"Failed to parse number of results from search response: {query_results}"
            return Result.Fail(error)

    def try_alternate_url(self):
        split = self.name.split()
        if len(split) <= 2:
            return Result.Fail(f"Failed to retrieve any results for player name: {self.name}")
        name_part = split[-1].upper()
        url = f"{MLB_PLAYER_SEARCH_URL}?sport_code='mlb'&name_part='{name_part}%25'&active_sw='Y'"
        return Result.Ok(url)

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
            "add_to_db_backup": True,
        }
        return Result.Ok(player_dict)

    def add_player_to_database(self, player_dict):
        try:
            new_player = db.Player(**player_dict)
            self.db_session.add(new_player)
            self.db_session.commit()
            player_id = db.PlayerId.find_by_mlb_id(self.db_session, new_player.mlb_id)
            if not player_id:
                name = f"{new_player.name_first} {new_player.name_last}"
                result = self.add_player_id_to_database(name, new_player.mlb_id, new_player.bbref_id)
                if result.failure:
                    return result
                player_id = result.value
            player_id.db_player_id = new_player.id
            self.db_session.commit()
            self.events.scrape_player_info_complete(new_player)
            return Result.Ok(new_player)
        except SQLAlchemyError as e:  # pragma: no cover
            return Result.Fail(f"Error: {repr(e)}")

    def add_player_id_to_database(self, name, mlb_id, bbref_id):
        try:
            player_id_dict = {
                "mlb_id": mlb_id,
                "mlb_name": name,
                "bbref_id": bbref_id,
            }
            new_player_id = db.PlayerId(**player_id_dict)
            self.db_session.add(new_player_id)
            self.db_session.commit()
            return Result.Ok(new_player_id)
        except SQLAlchemyError as e:  # pragma: no cover
            return Result.Fail(f"Error: {repr(e)}")
