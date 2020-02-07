"""Drop-in replacement for getattr function."""
from app.main.util.result import Result


def checkattr(obj, attr_name, default=None):
    """Acts exactly as getattr(), but extends to dict objects."""
    if type(obj) is dict and attr_name in obj:
        value = obj[attr_name]
        return Result.Ok(value)
    if hasattr(obj, attr_name):
        value = getattr(obj, attr_name)
        return Result.Ok(value)
    error = (
        "Error! Attribute does not exist:\n"
        "Object: {o}, Type: {t}, Attr Name: {a}".format(o=obj, t=type(obj), a=attr_name)
    )
    return Result.Fail(error)


def hasmethod(obj, method_name):
    """Return True if obj.method_name exists and is callable. Otherwise, return False."""
    obj_method = getattr(obj, method_name, None)
    return callable(obj_method) if obj_method else False
