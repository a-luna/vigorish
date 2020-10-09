"""Compiled regular expressions and patterns."""
import re

from vigorish.enums import DataSet, VigFile

TIMESTAMP_REGEX = re.compile(r"(?P<hour>\d?\d):(?P<minute>\d\d)")
JOB_NAME_PATTERN = r"^[\w\s-]+$"
JOB_NAME_REGEX = re.compile(JOB_NAME_PATTERN)
DATE_ONLY_TABLE_ID_REGEX = re.compile(r"(?P<year>\d{4,4})(?P<month>\d{2,2})(?P<day>\d{2,2})")
BBREF_BOXSCORE_URL_PATTERN = (
    r"^https:\/\/www.baseball-reference.com\/boxes\/"
    r"(?P<team_id>[A-Z]{3,3})\/(?P<game_id>[0-9A-Z]{12,12}).shtml$"
)
BBREF_BOXSCORE_URL_REGEX = re.compile(BBREF_BOXSCORE_URL_PATTERN)

BBREF_DAILY_JSON_FILENAME_REGEX = re.compile(
    r"bbref_games_for_date_(?P<year>\d{4,4})-(?P<month>\d{2,2})-(?P<day>\d{2,2})"
)
BB_DAILY_JSON_FILENAME_REGEX = re.compile(
    r"brooks_games_for_date_(?P<year>\d{4,4})-(?P<month>\d{2,2})-(?P<day>\d{2,2})"
)

PFX_TIMESTAMP_REGEX = re.compile(
    r"""
    (?P<year>\d{2,2})
    (?P<month>\d{2,2})
    (?P<day>\d{2,2})_
    (?P<hour>\d{2,2})
    (?P<minute>\d{2,2})
    (?P<second>\d{2,2})
    (?P<team_id>[a-z]{3,3})
""",
    re.VERBOSE,
)

BBREF_GAME_ID_REGEX = re.compile(
    r"""
    (?P<home_team>[A-Z]{3,3})
    (?P<year>\d{4,4})
    (?P<month>\d{2,2})
    (?P<day>\d{2,2})
    (?P<game_num>\d)
""",
    re.VERBOSE,
)

BBREF_GAME_ID_REGEX_STRICT = re.compile(
    r"""
    ^
    (?P<home_team>[A-Z]{3,3})
    (?P<year>\d{4,4})
    (?P<month>\d{2,2})
    (?P<day>\d{2,2})
    (?P<game_num>\d)
    $
""",
    re.VERBOSE,
)

BBREF_BOXSCORE_PATCH_LIST_FILENAME_REGEX = re.compile(
    r"""
    (?P<home_team>[A-Z]{3,3})
    (?P<year>\d{4,4})
    (?P<month>\d{2,2})
    (?P<day>\d{2,2})
    (?P<game_num>\d)
    _PATCH_LIST
""",
    re.VERBOSE,
)

PITCHFX_LOG_PATCH_LIST_FILENAME_REGEX = BBREF_BOXSCORE_PATCH_LIST_FILENAME_REGEX

COMBINED_DATA_FILENAME_REGEX = re.compile(
    r"""
    ^
    (?P<home_team>[A-Z]{3,3})
    (?P<year>\d{4,4})
    (?P<month>\d{2,2})
    (?P<day>\d{2,2})
    (?P<game_num>\d)
    _COMBINED_DATA
    $
""",
    re.VERBOSE,
)

BB_GAME_ID_REGEX = re.compile(
    r"""
    gid_
    (?P<year>\d{4,4})_
    (?P<month>\d{2,2})_
    (?P<day>\d{2,2})_
    (?P<away_team>[a-z]+)mlb_
    (?P<home_team>[a-z]+)mlb_
    (?P<game_num>\d)
""",
    re.VERBOSE,
)

AT_BAT_ID_REGEX = re.compile(
    r"""
    (?P<home_team>[A-Z]{3,3})
    (?P<year>\d{4,4})
    (?P<month>\d{2,2})
    (?P<day>\d{2,2})
    (?P<game_num>\d)_
    (?P<inning>\d{1,2})_
    (?P<pitcher_team>[A-Z]{3,3})_
    (?P<pitcher_mlb_id>\d{6,6})_
    (?P<batter_team>[A-Z]{3,3})_
    (?P<batter_mlb_id>\d{6,6})_
    (?P<at_bat_num>\d)
""",
    re.VERBOSE,
)

PITCH_APP_REGEX = re.compile(
    r"""
    (?P<home_team>[A-Z]{3,3})
    (?P<year>\d{4,4})
    (?P<month>\d{2,2})
    (?P<day>\d{2,2})
    (?P<game_num>\d)
    _(?P<mlb_id>\d{6,6})
""",
    re.VERBOSE,
)

INNING_LABEL_REGEX = re.compile(
    r"""
    (?P<inning_half>TOP|BOT)
    (?P<inning_number>[0-9]{2,2})
""",
    re.VERBOSE,
)

URL_ID_REGEX = {
    VigFile.SCRAPED_HTML: {
        DataSet.BROOKS_GAMES_FOR_DATE: DATE_ONLY_TABLE_ID_REGEX,
        DataSet.BROOKS_PITCH_LOGS: PITCH_APP_REGEX,
        DataSet.BROOKS_PITCHFX: PITCH_APP_REGEX,
        DataSet.BBREF_GAMES_FOR_DATE: DATE_ONLY_TABLE_ID_REGEX,
        DataSet.BBREF_BOXSCORES: BBREF_GAME_ID_REGEX_STRICT,
    },
    VigFile.PARSED_JSON: {
        DataSet.BROOKS_GAMES_FOR_DATE: BB_DAILY_JSON_FILENAME_REGEX,
        DataSet.BROOKS_PITCH_LOGS: BB_GAME_ID_REGEX,
        DataSet.BROOKS_PITCHFX: PITCH_APP_REGEX,
        DataSet.BBREF_GAMES_FOR_DATE: BBREF_DAILY_JSON_FILENAME_REGEX,
        DataSet.BBREF_BOXSCORES: BBREF_GAME_ID_REGEX_STRICT,
    },
    VigFile.PATCH_LIST: {
        DataSet.BROOKS_PITCHFX: PITCHFX_LOG_PATCH_LIST_FILENAME_REGEX,
        DataSet.BBREF_BOXSCORES: BBREF_BOXSCORE_PATCH_LIST_FILENAME_REGEX,
    },
    VigFile.COMBINED_GAME_DATA: {DataSet.ALL: COMBINED_DATA_FILENAME_REGEX},
}

URL_ID_CONVERT_REGEX = {
    VigFile.SCRAPED_HTML: {
        DataSet.BROOKS_GAMES_FOR_DATE: DATE_ONLY_TABLE_ID_REGEX,
        DataSet.BBREF_GAMES_FOR_DATE: DATE_ONLY_TABLE_ID_REGEX,
    },
    VigFile.PARSED_JSON: {
        DataSet.BROOKS_GAMES_FOR_DATE: BB_DAILY_JSON_FILENAME_REGEX,
        DataSet.BBREF_GAMES_FOR_DATE: BBREF_DAILY_JSON_FILENAME_REGEX,
    },
}
