"""Compiled regular expressions and patterns."""
import re


PITCH_APP_REGEX = re.compile(r"(?P<game_id>[0-9A-Z]{12,12})_(?P<mlb_id>\d{6,6})")
