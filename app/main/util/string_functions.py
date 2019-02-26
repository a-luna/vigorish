import re
import string

from fuzzywuzzy import process

from app.main.util.regex import TIMESTAMP_REGEX

def string_is_null_or_blank(s):
    """Check if a string is null or consists entirely of whitespace."""
    return not s or s.isspace()

def fuzzy_match(s, choices):
    choices_norm = [normalize(c) for c in choices]
    return process.extractOne(normalize(s), choices_norm)

def normalize(s):
    for p in string.punctuation:
        s = s.replace(p, '')
    s = s.lower().strip()
    return re.sub('\s+', ' ', s)

def convert_job_status_for_display(job_status):
    if 'rq' in job_status:
        return 'RQ Fetch Error'
    return  ' '.join([s.title() for s in job_status.split('_')])

def parse_timestamp(input):
    if string_is_null_or_blank(input):
        return None
    matches = TIMESTAMP_REGEX.findall(input)
    if not matches:
        return None
    time_str = matches[0]
    split = time_str.split(':')
    if len(split) != 2:
        return None
    return dict(hour=int(split[0]), minute=int(split[1]))