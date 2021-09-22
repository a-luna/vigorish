import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import PosixPath
from uuid import UUID

from vigorish.scrape.bbref_games_for_date.models.game_info import BBRefGameInfo
from vigorish.util.datetime_util import TIME_ZONE_LA
from vigorish.util.json_types import CustomJsonEncoder, decode

dec_1 = Decimal("0.045")
dec_2 = Decimal("-100000000000.01734")
complex_1 = complex("4+3j")
complex_2 = complex(4, 3)
dt_naive_1 = datetime(2021, 4, 1, 7, 30, 30)
dt_aware_1 = datetime(2021, 4, 1, 7, 30, 30, tzinfo=timezone.utc)
dt_aware_2 = datetime(2021, 4, 1, 7, 30, 30, tzinfo=TIME_ZONE_LA)
date_1 = dt_naive_1.date()
uuid_1 = UUID("e38ad2f6-6b2a-4de5-a5b8-4e04fc98231c")
path_1 = PosixPath("test.txt")
game_1 = BBRefGameInfo(
    url="https://www.baseball-reference.com/boxes/KCA/KCA202107270.shtml", bbref_game_id="KCA202107270"
)


def test_roundtrip_decimal_value():
    dec_1_dict = {
        "__class__": "Decimal",
        "__module__": "decimal",
        "decimal_sign": 0,
        "decimal_digits": [4, 5],
        "decimal_exponent": -3,
    }
    dec_1_json = json.dumps(dec_1, cls=CustomJsonEncoder)
    assert dec_1_json == str(dec_1_dict).replace("'", '"')
    dec_1_decoded = json.loads(dec_1_json, object_hook=decode)
    assert dec_1_decoded == dec_1

    dec_2_dict = {
        "__class__": "Decimal",
        "__module__": "decimal",
        "decimal_sign": 1,
        "decimal_digits": [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 7, 3, 4],
        "decimal_exponent": -5,
    }
    dec_2_json = json.dumps(dec_2, cls=CustomJsonEncoder)
    assert dec_2_json == str(dec_2_dict).replace("'", '"')
    dec_2_decoded = json.loads(dec_2_json, object_hook=decode)
    assert dec_2_decoded == dec_2


def test_roundtrip_complex_value():
    complex_dict = {"__class__": "complex", "real": 4.0, "imag": 3.0}
    complex_1_json = json.dumps(complex_1, cls=CustomJsonEncoder)
    assert complex_1_json == str(complex_dict).replace("'", '"')
    complex_2_json = json.dumps(complex_2, cls=CustomJsonEncoder)
    assert complex_2_json == str(complex_dict).replace("'", '"')
    complex_1_decoded = json.loads(complex_1_json, object_hook=decode)
    assert complex_1_decoded == complex_1
    complex_2_decoded = json.loads(complex_2_json, object_hook=decode)
    assert complex_2_decoded == complex_2


def test_roundtrip_datetime_value():
    dt_naive_1_dict = {"__class__": "datetime", "datetime_str": "2021-04-01T07:30:30"}
    dt_naive_1_json = json.dumps(dt_naive_1, cls=CustomJsonEncoder)
    assert dt_naive_1_json == str(dt_naive_1_dict).replace("'", '"')
    dt_naive_1_decoded = json.loads(dt_naive_1_json, object_hook=decode)
    assert dt_naive_1_decoded == dt_naive_1

    dt_aware_1_dict = {"__class__": "datetime", "datetime_str": "2021-04-01T07:30:30+0000"}
    dt_aware_1_json = json.dumps(dt_aware_1, cls=CustomJsonEncoder)
    assert dt_aware_1_json == str(dt_aware_1_dict).replace("'", '"')
    dt_aware_1_decoded = json.loads(dt_aware_1_json, object_hook=decode)
    assert dt_aware_1_decoded == dt_aware_1

    dt_aware_2_dict = {"__class__": "datetime", "datetime_str": "2021-04-01T07:30:30-0700"}
    dt_aware_2_json = json.dumps(dt_aware_2, cls=CustomJsonEncoder)
    assert dt_aware_2_json == str(dt_aware_2_dict).replace("'", '"')
    dt_aware_2_decoded = json.loads(dt_aware_2_json, object_hook=decode)
    assert dt_aware_2_decoded == dt_aware_2


def test_roundtrip_date_value():
    date_1_dict = {"__class__": "date", "date_str": "2021-04-01"}
    date_1_json = json.dumps(date_1, cls=CustomJsonEncoder)
    assert date_1_json == str(date_1_dict).replace("'", '"')
    date_1_decoded = json.loads(date_1_json, object_hook=decode)
    assert date_1_decoded == date_1


def test_roundtrip_uuid_value():
    uuid_1_dict = {"__class__": "UUID", "__module__": "uuid", "uuid_str": "e38ad2f6-6b2a-4de5-a5b8-4e04fc98231c"}
    uuid_1_json = json.dumps(uuid_1, cls=CustomJsonEncoder)
    assert uuid_1_json == str(uuid_1_dict).replace("'", '"')
    uuid_1_decoded = json.loads(uuid_1_json, object_hook=decode)
    assert uuid_1_decoded == uuid_1


def test_roundtrip_path_value():
    path_1_dict = {"__class__": "PosixPath", "__module__": "pathlib", "path_str": "test.txt"}
    path_1_json = json.dumps(path_1, cls=CustomJsonEncoder)
    assert path_1_json == str(path_1_dict).replace("'", '"')
    path_1_decoded = json.loads(path_1_json, object_hook=decode)
    assert path_1_decoded == path_1


def test_roundtrip_domain_object():
    game_1_dict = {
        "__class__": "BBRefGameInfo",
        "__module__": "vigorish.scrape.bbref_games_for_date.models.game_info",
        "url": "https://www.baseball-reference.com/boxes/KCA/KCA202107270.shtml",
        "bbref_game_id": "KCA202107270",
    }
    game_1_json = json.dumps(game_1, cls=CustomJsonEncoder)
    assert game_1_json == str(game_1_dict).replace("'", '"')
    game_1_decoded = json.loads(game_1_json, object_hook=decode)
    assert game_1_decoded == game_1


def test_roundtrip_nested_object():
    obj_1 = {
        "integer": 123,
        "float": 123.01,
        "string": "this is a string value",
        "boolean": False,
        "dec1": dec_1,
        "dec2": dec_2,
        "complex1": complex_1,
        "complex2": complex_2,
        "dtnaive1": dt_naive_1,
        "dtaware1": dt_aware_1,
        "dtaware2": dt_aware_2,
        "date": date_1,
        "uuid": uuid_1,
        "path": path_1,
        "game_info": game_1,
    }

    obj_1_json = json.dumps(obj_1, cls=CustomJsonEncoder, indent=2, sort_keys=False)
    obj_1_decoded = json.loads(obj_1_json, object_hook=decode)
    assert obj_1_decoded == obj_1

    obj_2 = {
        "dict1": obj_1,
        "dict2": {
            "boolean": False,
            "dec2": dec_2,
            "dtaware2": dt_aware_2,
        },
        "list1": [game_1, dec_1, complex_2],
    }

    obj_2_json = json.dumps(obj_2, cls=CustomJsonEncoder, indent=2, sort_keys=False)
    obj_2_decoded = json.loads(obj_2_json, object_hook=decode)
    assert obj_2_decoded == obj_2
