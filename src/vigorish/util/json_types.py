import json
import time
from collections.abc import Iterable, Mapping
from datetime import date, datetime
from decimal import Decimal, DecimalTuple
from importlib import import_module
from pathlib import PosixPath
from uuid import UUID

from dateutil.parser import parse

from vigorish.util.dt_format_strings import DATE_ONLY, ISO_8601


class CustomJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        obj_dict = {"__class__": obj.__class__.__name__}

        if isinstance(obj, complex):
            obj_dict["real"] = obj.real
            obj_dict["imag"] = obj.imag
            return obj_dict

        if isinstance(obj, datetime):
            obj_dict["datetime_str"] = obj.strftime(ISO_8601)
            return obj_dict

        if isinstance(obj, date):
            obj_dict["date_str"] = obj.strftime(DATE_ONLY)
            return obj_dict

        if isinstance(obj, time.struct_time):
            obj_dict["time_str"] = datetime.fromtimestamp(time.mktime(obj)).strftime(ISO_8601)
            return obj_dict

        obj_dict["__module__"] = obj.__module__

        if isinstance(obj, Decimal):
            decimal_tuple = obj.as_tuple()
            obj_dict["decimal_sign"] = decimal_tuple.sign
            obj_dict["decimal_digits"] = decimal_tuple.digits
            obj_dict["decimal_exponent"] = decimal_tuple.exponent
            return obj_dict

        if isinstance(obj, UUID):
            obj_dict["uuid_str"] = str(obj)
            return obj_dict

        if isinstance(obj, PosixPath):
            obj_dict["path_str"] = str(obj)
            return obj_dict

        if obj_dict["__module__"].startswith("vigorish"):
            obj_dict.update(obj.__dict__)
            return obj_dict

        if isinstance(obj, Mapping):
            return "{" + ", ".join(f"{self.encode(k)}: {self.encode(v)}" for (k, v) in obj.items()) + "}"
        if isinstance(obj, Iterable) and (not isinstance(obj, str)):
            return "[" + ", ".join(map(self.encode, obj)) + "]"

        return json.JSONEncoder.default(self, obj)


def decode(obj_dict):

    if "__class__" not in obj_dict:
        return obj_dict
    class_name = obj_dict.pop("__class__")

    if class_name == Decimal.__name__:
        dtuple = DecimalTuple(obj_dict["decimal_sign"], obj_dict["decimal_digits"], obj_dict["decimal_exponent"])
        return Decimal(dtuple)

    if class_name == complex.__name__:
        return complex(obj_dict["real"], obj_dict["imag"])

    if class_name == datetime.__name__:
        return parse(obj_dict["datetime_str"])

    if class_name == date.__name__:
        return parse(obj_dict["date_str"]).date()

    if class_name == time.struct_time.__name__:
        return parse(obj_dict["time_str"]).timetuple()

    if class_name == UUID.__name__:
        return UUID(obj_dict["uuid_str"])

    if class_name == PosixPath.__name__:
        return PosixPath(obj_dict["path_str"])

    if "__module__" not in obj_dict:
        return obj_dict

    try:
        module = import_module(obj_dict.pop("__module__"))
        cls = getattr(module, class_name)
        args = {k: v for k, v in obj_dict.items()}
        return cls(**args)
    except TypeError:
        pass
    return obj_dict
