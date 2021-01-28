from datetime import datetime

from vigorish.database import Player
from vigorish.tasks.scrape_mlb_player_info import ScrapeMlbPlayerInfoTask
from vigorish.util.result import Result

PLAYER_NAME = "Bryse Wilson"
GAME_DATE = datetime(2018, 8, 20)
BBREF_ID = "wilsobr02"
MOCK_API_RESPONSE = {
    "search_player_all": {
        "queryResults": {
            "created": "2021-01-09T08:24:28",
            "totalSize": "7",
            "row": [
                {
                    "position": "P",
                    "birth_country": "Saudi Arabia",
                    "weight": "227",
                    "birth_state": "",
                    "name_display_first_last": "Alex Wilson",
                    "college": "Texas A&M",
                    "height_inches": "0",
                    "name_display_roster": "Wilson",
                    "sport_code": "mlb",
                    "bats": "R",
                    "name_first": "Alex",
                    "team_code": "mil",
                    "birth_city": "Dhahran",
                    "height_feet": "6",
                    "pro_debut_date": "2013-04-11T00:00:00",
                    "team_full": "Milwaukee Brewers",
                    "team_abbrev": "MIL",
                    "birth_date": "1986-11-03T00:00:00",
                    "throws": "R",
                    "league": "NL",
                    "name_display_last_first": "Wilson, Alex",
                    "position_id": "1",
                    "high_school": "Hurricane, WV",
                    "name_use": "Alex",
                    "player_id": "543935",
                    "name_last": "Wilson",
                    "team_id": "158",
                    "service_years": "",
                    "active_sw": "Y",
                },
                {
                    "position": "C",
                    "birth_country": "USA",
                    "weight": "230",
                    "birth_state": "FL",
                    "name_display_first_last": "Bobby Wilson",
                    "college": "St. Petersburg JC, FL",
                    "height_inches": "0",
                    "name_display_roster": "Wilson, B",
                    "sport_code": "mlb",
                    "bats": "R",
                    "name_first": "Bobby",
                    "team_code": "det",
                    "birth_city": "Dunedin",
                    "height_feet": "6",
                    "pro_debut_date": "2008-04-28T00:00:00",
                    "team_full": "Detroit Tigers",
                    "team_abbrev": "DET",
                    "birth_date": "1983-04-08T00:00:00",
                    "throws": "R",
                    "league": "AL",
                    "name_display_last_first": "Wilson, Bobby",
                    "position_id": "2",
                    "high_school": "Seminole, FL",
                    "name_use": "Bobby",
                    "player_id": "435064",
                    "name_last": "Wilson",
                    "team_id": "116",
                    "service_years": "",
                    "active_sw": "Y",
                },
                {
                    "position": "P",
                    "birth_country": "USA",
                    "weight": "225",
                    "birth_state": "NC",
                    "name_display_first_last": "Bryse Wilson",
                    "college": "",
                    "height_inches": "2",
                    "name_display_roster": "Wilson, Bry",
                    "sport_code": "mlb",
                    "bats": "R",
                    "name_first": "Bryse",
                    "team_code": "atl",
                    "birth_city": "Durham",
                    "height_feet": "6",
                    "pro_debut_date": "2018-08-20T00:00:00",
                    "team_full": "Atlanta Braves",
                    "team_abbrev": "ATL",
                    "birth_date": "1997-12-20T00:00:00",
                    "throws": "R",
                    "league": "NL",
                    "name_display_last_first": "Wilson, Bryse",
                    "position_id": "1",
                    "high_school": "Orange, Hillsborough, NC",
                    "name_use": "Bryse",
                    "player_id": "669060",
                    "name_last": "Wilson",
                    "team_id": "144",
                    "service_years": "",
                    "active_sw": "Y",
                },
                {
                    "position": "3B",
                    "birth_country": "USA",
                    "weight": "219",
                    "birth_state": "TN",
                    "name_display_first_last": "Jacob Wilson",
                    "college": "Memphis",
                    "height_inches": "11",
                    "name_display_roster": "Wilson",
                    "sport_code": "mlb",
                    "bats": "R",
                    "name_first": "Jacob",
                    "team_code": "oak",
                    "birth_city": "Bartlett",
                    "height_feet": "5",
                    "pro_debut_date": "",
                    "team_full": "Oakland Athletics",
                    "team_abbrev": "OAK",
                    "birth_date": "1990-07-29T00:00:00",
                    "throws": "R",
                    "league": "AL",
                    "name_display_last_first": "Wilson, Jacob",
                    "position_id": "5",
                    "high_school": "Bartlett, TN",
                    "name_use": "Jacob",
                    "player_id": "607111",
                    "name_last": "Wilson",
                    "team_id": "133",
                    "service_years": "",
                    "active_sw": "Y",
                },
                {
                    "position": "P",
                    "birth_country": "USA",
                    "weight": "205",
                    "birth_state": "CA",
                    "name_display_first_last": "Justin Wilson",
                    "college": "Fresno State",
                    "height_inches": "2",
                    "name_display_roster": "Wilson, J",
                    "sport_code": "mlb",
                    "bats": "L",
                    "name_first": "Justin",
                    "team_code": "nyn",
                    "birth_city": "Anaheim",
                    "height_feet": "6",
                    "pro_debut_date": "2012-08-20T00:00:00",
                    "team_full": "New York Mets",
                    "team_abbrev": "NYM",
                    "birth_date": "1987-08-18T00:00:00",
                    "throws": "L",
                    "league": "NL",
                    "name_display_last_first": "Wilson, Justin",
                    "position_id": "1",
                    "high_school": "Dr. Floyd B. Buchanan, Clovis, CA",
                    "name_use": "Justin",
                    "player_id": "458677",
                    "name_last": "Wilson",
                    "team_id": "121",
                    "service_years": "",
                    "active_sw": "Y",
                },
                {
                    "position": "CF",
                    "birth_country": "USA",
                    "weight": "198",
                    "birth_state": "CA",
                    "name_display_first_last": "Marcus Wilson",
                    "college": "",
                    "height_inches": "2",
                    "name_display_roster": "Wilson",
                    "sport_code": "mlb",
                    "bats": "R",
                    "name_first": "Marcus",
                    "team_code": "bos",
                    "birth_city": "Los Angeles",
                    "height_feet": "6",
                    "pro_debut_date": "",
                    "team_full": "Boston Red Sox",
                    "team_abbrev": "BOS",
                    "birth_date": "1996-08-15T00:00:00",
                    "throws": "R",
                    "league": "AL",
                    "name_display_last_first": "Wilson, Marcus",
                    "position_id": "8",
                    "high_school": "Junipero Serra, Gardena, CA",
                    "name_use": "Marcus",
                    "player_id": "657129",
                    "name_last": "Wilson",
                    "team_id": "111",
                    "service_years": "",
                    "active_sw": "Y",
                },
                {
                    "position": "C",
                    "birth_country": "Venezuela",
                    "weight": "245",
                    "birth_state": "",
                    "name_display_first_last": "Wilson Ramos",
                    "college": "",
                    "height_inches": "1",
                    "name_display_roster": "Ramos, W",
                    "sport_code": "mlb",
                    "bats": "R",
                    "name_first": "Wilson",
                    "team_code": "nyn",
                    "birth_city": "Valencia",
                    "height_feet": "6",
                    "pro_debut_date": "2010-05-02T00:00:00",
                    "team_full": "New York Mets",
                    "team_abbrev": "NYM",
                    "birth_date": "1987-08-10T00:00:00",
                    "throws": "R",
                    "league": "NL",
                    "name_display_last_first": "Ramos, Wilson",
                    "position_id": "2",
                    "high_school": "U.E. Santa Ines, Carabobo, VEN",
                    "name_use": "Wilson",
                    "player_id": "467092",
                    "name_last": "Ramos",
                    "team_id": "121",
                    "service_years": "",
                    "active_sw": "Y",
                },
            ],
        },
    }
}


class MockResponse:
    @staticmethod
    def json():
        return MOCK_API_RESPONSE


def test_scrape_mlb_player_info(vig_app, mocker):
    def mock_request_url(url):
        return Result.Ok(MockResponse())

    mocker.patch("vigorish.tasks.scrape_mlb_player_info.request_url_with_retries", mock_request_url)
    scrape_player_info_task = ScrapeMlbPlayerInfoTask(vig_app)
    result = scrape_player_info_task.execute(PLAYER_NAME, BBREF_ID, GAME_DATE)
    assert result.success
    new_player = result.value
    assert new_player and isinstance(new_player, Player)
    assert new_player.name_first == "Bryse"
    assert new_player.name_last == "Wilson"
    assert new_player.throws == "R"
    assert new_player.bats == "R"
    assert new_player.weight == 225
    assert new_player.height == 74
    assert new_player.debut == GAME_DATE
    assert new_player.birth_year == 1997
    assert new_player.birth_month == 12
    assert new_player.birth_day == 20
    assert new_player.birth_country == "USA"
    assert new_player.birth_state == "NC"
    assert new_player.birth_city == "Durham"
    assert new_player.bbref_id == BBREF_ID
    assert new_player.mlb_id == 669060
