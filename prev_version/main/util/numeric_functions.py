import math


def is_odd(x):
    return x & 1 == 1


def is_even(x):
    return x & 1 == 0


def is_nan(x):
    return isinstance(x, float) and math.isnan(x)


def sanitize(x):
    if math.isnan(x):
        return 0
    return x
