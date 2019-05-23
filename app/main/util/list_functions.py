"""Functions which operator on or produce list objects."""
import itertools
from collections import defaultdict
from datetime import datetime

from app.main.util.dt_format_strings import DT_STR_FORMAT_ALL


def as_list_of_dicts(db_objects):
    """Convert a list of model objects to a list of dicts."""
    return [obj.as_dict() for obj in db_objects]


def display_dict(
    dict,
    title='',
    title_prefix='[',
    title_suffix=']',
    extra_dots=2
):
    """Pretty print all dict keys and values, title is optional."""
    max_length = 0
    for k, v in dict.items():
        if not v:
            if type(v) is not int and type(v) is not bool:
                continue
        if len(k) > max_length:
            max_length = len(k)
    max_length += extra_dots

    if title:
        print(f'\n{title_prefix}{title}{title_suffix}')
    else:
        print('')
    for k, v in dict.items():
        if not v:
            if type(v) is not int and type(v) is not bool:
                continue
        if type(v) is datetime:
            v = v.strftime(DT_STR_FORMAT_ALL)
        c = max_length - len(k)
        d = '.' * c
        print('{k}{d}: {v}'.format(k=k, d=d, v=v))

def report_dict(
    dict,
    title='',
    title_prefix='[',
    title_suffix=']',
    extra_dots=2
):
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
        report += f'{title_prefix}{title}{title_suffix}'
    for k, v in dict.items():
        if not v:
            if type(v) is not int and type(v) is not bool:
                continue
        if type(v) is datetime:
            v = v.strftime(DT_STR_FORMAT_ALL)
        c = max_length - len(k)
        d = '.' * c
        report += f'\n{k}{d}: {v}'
    return report


def filter_dict(source_dict, keys):
    """Create a dict which is a subset of source_dict."""
    return {k:source_dict[k] for k in source_dict.keys() & keys}


def flatten_list2d(list2d):
    """Product a normal list by flattenning a 2-dimensional list."""
    return list(itertools.chain(*list2d))


def group_and_sort_list(
    unsorted,
    group_attr,
    sort_attr,
    sort_groups_desc=False,
    sort_all_desc=False
):
    """Product a list that is grouped and sorted by attributes specified."""
    list_sorted = sorted(
        unsorted,
        key=lambda x: getattr(x, sort_attr),
        reverse=sort_all_desc
    )
    list_grouped = defaultdict(list)
    for item in list_sorted:
        list_grouped[getattr(item, group_attr)].append(item)
    list_grouped_sorted = []
    for group in sorted(list_grouped.keys(), reverse=sort_groups_desc):
        list_grouped_sorted.extend(list_grouped[group])
    return list_grouped_sorted

def compare_lists(list1, list2):
    check1 = not list(set(sorted(list1)) - set(sorted(list2)))
    check2 = not list(set(sorted(list2)) - set(sorted(list1)))
    return check1 and check2

def print_list(l):
    s = ''
    for i in range(0, len(l)):
        if i != len(l) - 1:
            s += f'{i+1}) {l[i]},\n'
        else:
            s += f'{i+1}) {l[i]}'
    return s
