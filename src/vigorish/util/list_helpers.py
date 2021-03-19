"""Functions which operate on or produce list objects."""
import itertools
from collections import defaultdict, OrderedDict
from datetime import datetime
from random import randint
from typing import Dict

from vigorish.util.dt_format_strings import DT_AWARE


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
        if not v and type(v) is not int and type(v) is not bool:
            continue
        if len(k) > max_length:
            max_length = len(k)
    max_length += extra_dots

    if title:
        report += f"{title_prefix}{title}{title_suffix}"
    for k, v in dict.items():
        if not v and type(v) is not int and type(v) is not bool:
            continue
        if type(v) is datetime:
            v = v.strftime(DT_AWARE)
        c = max_length - len(k)
        report += f"\n{k}{'.' * c}: {v}"
    return report.strip()


def dict_to_param_list(input_dict: Dict):
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


def group_and_sort_list(unsorted, group_attr, sort_attr, sort_groups_desc=False, sort_all_desc=False):
    list_sorted = sorted(unsorted, key=lambda x: getattr(x, sort_attr), reverse=sort_all_desc)
    list_grouped = defaultdict(list)
    for item in list_sorted:
        list_grouped[getattr(item, group_attr)].append(item)
    grouped_sorted = OrderedDict()
    for group in sorted(list_grouped.keys(), reverse=sort_groups_desc):
        grouped_sorted[group] = list_grouped[group]
    return grouped_sorted


def group_and_sort_dict_list(unsorted, group_key, sort_key, sort_groups_desc=False, sort_all_desc=False):
    list_sorted = sorted(unsorted, key=lambda x: x[sort_key], reverse=sort_all_desc)
    list_grouped = defaultdict(list)
    for item in list_sorted:
        list_grouped[item[group_key]].append(item)
    grouped_sorted = OrderedDict()
    for group in sorted(list_grouped.keys(), reverse=sort_groups_desc):
        grouped_sorted[group] = list_grouped[group]
    return grouped_sorted


def compare_lists(list1, list2):
    check1 = not list(set(list1) - set(list2))
    check2 = not list(set(list2) - set(list1))
    return check1 and check2


def make_chunked_list(input_list, chunk_size):
    chunked_list = []
    (total_chunks, last_chunk_size) = divmod(len(input_list), chunk_size)
    if last_chunk_size:
        total_chunks += 1
    for i in range(total_chunks):
        start = i * chunk_size
        end = start + chunk_size
        if last_chunk_size and i == total_chunks - 1:
            end = len(input_list)
        chunked_list.append(input_list[start:end])
    return chunked_list


def make_irregular_chunked_list(input_list, min_chunk_size, max_chunk_size):
    chunked_list = []
    remaining = len(input_list)
    end = 0
    while remaining:
        if remaining > max_chunk_size:
            chunk_size = randint(min_chunk_size, max_chunk_size)
        else:
            chunk_size = remaining
        start = end
        end += chunk_size
        chunked_list.append(input_list[start:end])
        remaining -= chunk_size
    return chunked_list
