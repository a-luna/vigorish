from datetime import datetime

from vigorish.util.list_helpers import (
    display_dict,
    dict_to_param_list,
    compare_lists,
    make_chunked_list,
    make_irregular_chunked_list,
)


def test_display_dict(capfd):
    results = {
        "success": True,
        "result": 742,
        "message": "this is a fantastic test case!",
        "did_change": False,
        "count": 0,
        "exec_time": datetime(2019, 12, 31, 17, 17, 22),
        "errors": None,
    }
    display_dict(results, title="DICTIONARY CONTENTS", title_prefix="### ", title_suffix=" ###")
    out, err = capfd.readouterr()
    assert "### DICTIONARY CONTENTS ###" in out
    assert "success.....: True" in out
    assert "result......: 742" in out
    assert "message.....: this is a fantastic test case!" in out
    assert "did_change..: False" in out
    assert "count.......: 0" in out
    assert "exec_time...: 12/31/2019 05:17:22 PM" in out
    assert "errors" not in out
    assert not err


def test_dict_to_param_list():
    params = {"verbose": True, "timeout": 10, "url": "https://www.google.com"}
    param_list = dict_to_param_list(params)
    assert param_list == "--verbose --timeout=10 --url=https://www.google.com"


def test_compare_lists():
    list1 = ["a", "b", "c", "d"]
    list2 = ["d", "c", "b", "a"]
    matches = compare_lists(list1, list2)
    assert matches

    list1 = ["a", "b", "c", "d", "a"]
    list2 = ["d", "c", "b", "a"]
    matches = compare_lists(list1, list2)
    assert matches

    list1 = ["a", "b", "c", "d", "e"]
    list2 = ["d", "c", "b", "a"]
    matches = compare_lists(list1, list2)
    assert not matches


def test_make_chunked_list():
    input_list = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    chunked = make_chunked_list(input_list, chunk_size=3)
    assert len(chunked) == 3
    assert len(chunked[0]) == 3
    assert chunked[0] == [1, 2, 3]
    assert len(chunked[1]) == 3
    assert chunked[1] == [4, 5, 6]
    assert len(chunked[2]) == 3
    assert chunked[2] == [7, 8, 9]

    chunked = make_chunked_list(input_list, chunk_size=4)
    assert len(chunked) == 3
    assert len(chunked[0]) == 4
    assert chunked[0] == [1, 2, 3, 4]
    assert len(chunked[1]) == 4
    assert chunked[1] == [5, 6, 7, 8]
    assert len(chunked[2]) == 1
    assert chunked[2] == [9]


def test_make_irregular_chunked_list():
    min = 6
    max = 7
    input_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
    chunked = make_irregular_chunked_list(input_list, min_chunk_size=min, max_chunk_size=max)
    for num, chunk in enumerate(chunked, start=1):
        if num < len(chunked):
            assert len(chunk) <= max and len(chunk) >= min
