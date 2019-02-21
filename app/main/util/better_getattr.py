"""Drop-in replacement for getattr function."""

def better_getattr(obj, attr_name):
    """Acts exactly as getattr(), but extends to dict objects."""
    success = True
    message = 'Successfullly retrieved value.'
    if type(obj) is dict and attr_name in obj:
        value = obj[attr_name]
    elif hasattr(obj, attr_name):
        value = getattr(obj, attr_name)
    else:
        message = (
            'Error! Attribute does not exist:\n'
            'Object: {o}, Type: {t}, Attr Name: {a}'
            .format(o=obj, t=type(obj), a=attr_name)
        )
        success = False
    return {'success': success, 'result': value, 'message': message}
