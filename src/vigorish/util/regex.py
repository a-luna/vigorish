"""Compiled regular expressions and patterns."""
import re


PITCH_APP_REGEX = re.compile(r"(?P<game_id>[0-9A-Z]{12,12})_(?P<mlb_id>\d{6,6})")

BBREF_BOXSCORE_URL_PATTERN = (
    r"^https:\/\/www.baseball-reference.com\/boxes\/"
    r"(?P<team_id>[A-Z]{3,3})\/(?P<game_id>[0-9A-Z]{12,12}).shtml$"
)
BBREF_BOXSCORE_URL_REGEX = re.compile(BBREF_BOXSCORE_URL_PATTERN)
TIMESTAMP_REGEX = re.compile(r"(?P<hour>\d?\d):(?P<minute>\d\d)")
