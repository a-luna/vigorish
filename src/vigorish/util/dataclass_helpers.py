from datetime import datetime

from vigorish.util.dt_format_strings import DATE_ONLY, DT_AWARE


def dict_from_dataclass(obj, data_class, date_format=DT_AWARE):
    obj_dict = {}
    for name, field in data_class.__dataclass_fields__.items():
        value = getattr(obj, name, None)
        if field.type is int:
            obj_dict[name] = int(value)
        elif field.type is bool:
            obj_dict[name] = True if value else False
        elif not value:
            obj_dict[name] = None
        elif field.type is datetime:
            obj_dict[name] = value.strftime(date_format)
        elif field.type is float:
            obj_dict[name] = float(value)
        else:
            obj_dict[name] = value
    return obj_dict


def sanitize_row_dict(row_dict, date_format=DATE_ONLY):
    return [val_to_string(val, date_format) for val in row_dict.values()]


def val_to_string(val, date_format):
    return (
        val
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
