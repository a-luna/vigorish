"""Helper functions that handle and produce numeric values."""
import math


def is_nan(x):
    return isinstance(x, float) and math.isnan(x)


def sanitize(x):
    if math.isnan(x):
        return 0
    return x
