"""Type variables for various config settings and JSON dicts."""
from typing import Mapping, Union, Tuple

STR_ENUM_JSON_VALUE = Union[None, bool, str]
STR_ENUM_JSON_SETTING = Mapping[str, STR_ENUM_JSON_VALUE]
NUMERIC_OPTIONS_JSON_VALUE = Mapping[str, Union[None, bool, int]]
NUMERIC_JSON_VALUE = Union[bool, str, NUMERIC_OPTIONS_JSON_VALUE]
NUMERIC_JSON_SETTING = Mapping[str, NUMERIC_JSON_VALUE]
JSON_CONFIG_VALUE = Union[STR_ENUM_JSON_SETTING, NUMERIC_JSON_SETTING]
JSON_CONFIG_SETTING = Mapping[str, JSON_CONFIG_VALUE]
NUMERIC_PROMPT_VALUE = Tuple[bool, bool, int, int, int]
