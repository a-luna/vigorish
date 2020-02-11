"""Functions which operate on or produce list objects."""
from datetime import datetime
from typing import Dict, Any

from vigorish.util.datetime_format_strings import DT_STR_FORMAT_ALL


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
        report += f"\n{k}{d}: {v}"
    return report