from dataclasses import asdict
from datetime import datetime

from dacite import from_dict

from vigorish.util.dt_format_strings import DATE_ONLY


def serialize_data_class_to_csv(data_class_objects, date_format):
    dataclass_dicts = [asdict(do) for do in data_class_objects]
    if not dataclass_dicts:
        return None
    col_names = [",".join(list(dataclass_dicts[0].keys()))]
    csv_rows = [dict_to_csv_row(d, date_format) for d in dataclass_dicts]
    return "\n".join((col_names + csv_rows))


def deserialize_data_class_from_csv(csv_text, data_class):
    csv_rows = csv_text.split("\n")
    col_names = [col.strip() for col in csv_rows.pop(0).split(",")]
    csv_rows = [row.split(",") for row in csv_rows]
    csv_dict_list = [dict(zip(col_names, row)) for row in csv_rows if row != [""]]
    return [from_dict(data_class=data_class, data=csv_dict) for csv_dict in csv_dict_list]


def serialize_db_object_to_csv(db_obj, dataclass, date_format=DATE_ONLY):
    csv_dict = {}
    for name, field in dataclass.__dataclass_fields__.items():
        value = getattr(db_obj, name, None)
        if field.type is int and not isinstance(value, int):
            csv_dict[name] = int(value)
        elif field.type is float and not isinstance(value, float):
            csv_dict[name] = float(value)
        elif field.type is bool:
            csv_dict[name] = True if value else False
        elif not value:
            csv_dict[name] = None
        else:
            csv_dict[name] = value
    return dict_to_csv_row(csv_dict, date_format)


def dict_to_csv_row(csv_dict, date_format):
    return ",".join([sanitize_value_for_csv(val, date_format) for val in csv_dict.values()])


def sanitize_value_for_csv(val, date_format):
    return (
        val.replace(",", ";")
        if isinstance(val, str)
        else val.strftime(date_format).strip()
        if isinstance(val, datetime)
        else 0
        if (isinstance(val, bool) and not val)
        else 1
        if (isinstance(val, bool) and val)
        else ""
        if not val
        else str(val)
    )
