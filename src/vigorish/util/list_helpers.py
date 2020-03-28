"""Functions which operate on or produce list objects."""
import itertools
from collections import defaultdict, OrderedDict
from datetime import datetime
from typing import Dict, Any

from vigorish.util.dt_format_strings import DT_STR_FORMAT_ALL
from vigorish.util.string_helpers import wrap_text


def display_dict(dict, title="", title_prefix="[", title_suffix="]", extra_dots=2):
    """Pretty print all dict keys and values, title is optional."""
    report = report_dict(
        dict=dict,
        title=title,
        title_prefix=title_prefix,
        title_suffix=title_suffix,
        extra_dots=extra_dots,
    )
    print(report)


def report_dict(dict, title="", title_prefix="### ", title_suffix=" ###", extra_dots=2):
    """Pretty print all dict keys and values, title is optional."""
    report = "\n"
    max_length = 0
    for k, v in dict.items():
        if not v:
            if type(v) is not int and type(v) is not bool:
                continue
        if len(k) > max_length:
            max_length = len(k)
    max_length += extra_dots

    if title:
        report += f"{title_prefix}{title}{title_suffix}"
    for k, v in dict.items():
        if not v:
            if type(v) is not int and type(v) is not bool:
                continue
        if type(v) is datetime:
            v = v.strftime(DT_STR_FORMAT_ALL)
        c = max_length - len(k)
        d = "." * c
        dict_item = f"{k}{d}: {v}"
        report += f"\n{wrap_text(dict_item, max_len=70)}"
    return report.strip()


def dict_to_param_list(input_dict: Dict) -> str:
    params = []
    for name, value in input_dict.items():
        if isinstance(value, bool) and value:
            params.append(f"--{name}")
        else:
            params.append(f"--{name}={value:}")
    return " ".join(params)


def as_dict_list(db_objects):
    """Convert a list of model objects to a list of dicts."""
    return [obj.as_dict() for obj in db_objects if hasmethod(obj, "as_dict")]


def hasmethod(obj, method_name):
    """Return True if obj.method_name exists and is callable. Otherwise, return False."""
    obj_method = getattr(obj, method_name, None)
    return callable(obj_method) if obj_method else False


def flatten_list2d(list2d):
    """Produce a normal list by flattenning a 2-dimensional list."""
    return list(itertools.chain(*list2d))


def group_and_sort_list(
    unsorted, group_attr, sort_attr, sort_groups_desc=False, sort_all_desc=False
):
    list_sorted = sorted(unsorted, key=lambda x: getattr(x, sort_attr), reverse=sort_all_desc)
    list_grouped = defaultdict(list)
    for item in list_sorted:
        list_grouped[getattr(item, group_attr)].append(item)
    grouped_sorted = OrderedDict()
    for group in sorted(list_grouped.keys(), reverse=sort_groups_desc):
        grouped_sorted[group] = list_grouped[group]
    return grouped_sorted


def compare_lists(list1, list2):
    check1 = not list(set(sorted(list1)) - set(sorted(list2)))
    check2 = not list(set(sorted(list2)) - set(sorted(list1)))
    return check1 and check2