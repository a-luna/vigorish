"""Constant values that are referenced from multiple places."""
from vigorish.enums import DataSet

ENV_VAR_NAMES = [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_DEFAULT_REGION",
    "CONFIG_FILE",
    "DATABASE_URL",
]

TEAM_NAME_MAP = {
    "Arizona Diamondbacks": "ARI",
    "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS",
    "Chicago White Sox": "CHA",
    "Chicago Cubs": "CHN",
    "Cincinnati Reds": "CIN",
    "Cleveland Indians": "CLE",
    "Colorado Rockies": "COL",
    "Detroit Tigers": "DET",
    "Houston Astros": "HOU",
    "Kansas City Royals": "KCA",
    "Los Angeles Angels of Anaheim": "ANA",
    "Los Angeles Angels": "ANA",
    "Los Angeles Dodgers": "LAN",
    "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN",
    "New York Yankees": "NYA",
    "New York Mets": "NYN",
    "Oakland Athletics": "OAK",
    "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SDN",
    "Seattle Mariners": "SEA",
    "San Francisco Giants": "SFN",
    "St. Louis Cardinals": "SLN",
    "Tampa Bay Rays": "TBA",
    "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WAS",
}

BR_BB_TEAM_ID_MAP = {
    "CHW": "CHA",
    "CHC": "CHN",
    "KCR": "KCA",
    "LAA": "ANA",
    "LAD": "LAN",
    "NYY": "NYA",
    "NYM": "NYN",
    "SDP": "SDN",
    "SFG": "SFN",
    "STL": "SLN",
    "TBR": "TBA",
    "WSN": "WAS",
}

BB_BR_TEAM_ID_MAP = {v: k for k, v in BR_BB_TEAM_ID_MAP.items()}

PACIFIC_TZ_TEAMS = ["ANA", "ARI", "LAD", "OAK", "SDP", "SEA", "SFG"]

DEFENSE_POSITIONS = [
    "P",
    "C",
    "1B",
    "2B",
    "3B",
    "SS",
    "RF",
    "CF",
    "LF",
    "DH",
    "PH",
    "PR",
]

PPB_PITCH_LOG_DICT = {
    "C": {
        "description": "Called strike",
        "pitch_counts": 1,
        "did_swing": False,
        "made_contact": False,
    },
    "S": {
        "description": "Swinging strike",
        "pitch_counts": 1,
        "did_swing": True,
        "made_contact": False,
    },
    "F": {"description": "Foul", "pitch_counts": 1, "did_swing": True, "made_contact": True},
    "B": {"description": "Ball", "pitch_counts": 1, "did_swing": False, "made_contact": False},
    "X": {
        "description": "Ball put into play by batter",
        "pitch_counts": 1,
        "did_swing": True,
        "made_contact": True,
    },
    "T": {"description": "Foul tip", "pitch_counts": 1, "did_swing": True, "made_contact": True},
    "K": {
        "description": "Strike (unknown type)",
        "pitch_counts": 1,
        "did_swing": False,
        "made_contact": False,
    },
    "I": {
        "description": "Intentional ball",
        "pitch_counts": 1,
        "did_swing": False,
        "made_contact": False,
    },
    "H": {
        "description": "Hit batter",
        "pitch_counts": 1,
        "did_swing": False,
        "made_contact": False,
    },
    "L": {"description": "Foul bunt", "pitch_counts": 1, "did_swing": True, "made_contact": True},
    "M": {
        "description": "Missed bunt attempt",
        "pitch_counts": 1,
        "did_swing": True,
        "made_contact": False,
    },
    "N": {
        "description": "No pitch (on balks and interference calls)",
        "pitch_counts": 0,
        "did_swing": False,
        "made_contact": False,
    },
    "O": {
        "description": "Foul tip on bunt",
        "pitch_counts": 1,
        "did_swing": True,
        "made_contact": True,
    },
    "P": {"description": "Pitchout", "pitch_counts": 1, "did_swing": False, "made_contact": False},
    "Q": {
        "description": "Swinging on pitchout",
        "pitch_counts": 1,
        "did_swing": True,
        "made_contact": False,
    },
    "R": {
        "description": "Foul ball on pitchout",
        "pitch_counts": 1,
        "did_swing": True,
        "made_contact": True,
    },
    "U": {
        "description": "Unknown or missed pitch",
        "pitch_counts": 1,
        "did_swing": False,
        "made_contact": False,
    },
    "V": {
        "description": "(Called ball because pitcher went to his mouth)",
        "pitch_counts": 0,
        "did_swing": False,
        "made_contact": False,
    },
    "Y": {
        "description": "Ball put into play on pitchout",
        "pitch_counts": 1,
        "did_swing": True,
        "made_contact": True,
    },
    "1": {
        "description": "(Pickoff throw to first)",
        "pitch_counts": 0,
        "did_swing": False,
        "made_contact": False,
    },
    "2": {
        "description": "(Pickoff throw to second)",
        "pitch_counts": 0,
        "did_swing": False,
        "made_contact": False,
    },
    "3": {
        "description": "(Pickoff throw to third)",
        "pitch_counts": 0,
        "did_swing": False,
        "made_contact": False,
    },
    ">": {
        "description": "(Runner going on the pitch)",
        "pitch_counts": 0,
        "did_swing": False,
        "made_contact": False,
    },
    "+": {
        "description": "(Pickoff throw by the catcher)",
        "pitch_counts": 0,
        "did_swing": False,
        "made_contact": False,
    },
    "*": {
        "description": "(The following pitch was blocked by the catcher)",
        "pitch_counts": 0,
        "did_swing": False,
        "made_contact": False,
    },
    ".": {
        "description": "(Play not involving the batter)",
        "pitch_counts": 0,
        "did_swing": False,
        "made_contact": False,
    },
}

VENUE_TERMS = ["stadium", "park", "field", "coliseum", "centre", "estadio", "dome"]

ZONE_LOCATIONS_INSIDE_SZ = [6, 7, 8, 11, 12, 13, 16, 17, 18]

PITCH_DES_SWINGING_STRIKE = [
    "Foul Tip",
    "Missed Bunt",
    "Swinging Pitchout",
    "Swinging Strike",
    "Swinging Strike (Blocked)",
]

PITCH_DES_DID_SWING = [
    "Foul",
    "Foul (Runner Going)",
    "Foul Bunt",
    "Foul Tip",
    "In play, no out",
    "In play; no out",
    "In play, out(s)",
    "In play; out(s)",
    "In play, run(s)",
    "In play; run(s)",
    "Missed Bunt",
    "Swinging Pitchout",
    "Swinging Strike",
    "Swinging Strike (Blocked)",
]

PITCH_DES_MADE_CONTACT = [
    "Foul",
    "Foul (Runner Going)",
    "Foul Bunt",
    "Foul Tip",
    "In play, no out",
    "In play; no out",
    "In play, out(s)",
    "In play; out(s)",
    "In play, run(s)",
    "In play; run(s)",
]

AT_BAT_RESULTS_OUT = [
    "bunt groundout",
    "bunt pop out",
    "double play",
    "flyout",
    "forceout",
    "grounded into dp",
    "groundout",
    "lineout",
    "pop out",
    "runner out",
    "sac bunt",
    "sac fly",
    "sac fly dp",
    "sacrifice bunt dp",
    "strikeout",
    "strikeout - dp",
    "triple play",
]

AT_BAT_RESULTS_UNCLEAR = [
    "batter interference",
    "catcher interference",
    "fan interference",
    "fielders choice",
    "fielders choice out",
    "missing_des",
]

AT_BAT_RESULTS_HIT = [
    "double",
    "home run",
    "single",
    "triple",
]

AT_BAT_RESULTS_WALK = [
    "intent walk",
    "walk",
]

AT_BAT_RESULTS_STRIKEOUT = [
    "strikeout",
    "strikeout - dp",
]

AT_BAT_RESULTS_HBP = [
    "hit by pitch",
]

AT_BAT_RESULTS_ERROR = [
    "field error",
]

AT_BAT_RESULTS_SAC_HIT = [
    "sac bunt",
    "sacrifice bunt dp",
]

AT_BAT_RESULTS_SAC_FLY = [
    "sac fly",
    "sac fly dp",
]

BARREL_MAP = {
    98: {"min": 26, "max": 30},
    99: {"min": 25, "max": 31},
    100: {"min": 24, "max": 33},
    101: {"min": 23, "max": 34},
    102: {"min": 22, "max": 35},
    103: {"min": 21, "max": 36},
    104: {"min": 20, "max": 37},
    105: {"min": 19, "max": 38},
    106: {"min": 18, "max": 39},
    107: {"min": 17, "max": 40},
    108: {"min": 16, "max": 41},
    109: {"min": 15, "max": 43},
    110: {"min": 14, "max": 44},
    111: {"min": 13, "max": 45},
    112: {"min": 12, "max": 46},
    113: {"min": 11, "max": 47},
    114: {"min": 10, "max": 48},
    115: {"min": 9, "max": 49},
    116: {"min": 8, "max": 50},
}

JOB_SPINNER_COLORS = {
    DataSet.BBREF_GAMES_FOR_DATE: "red",
    DataSet.BROOKS_GAMES_FOR_DATE: "blue",
    DataSet.BBREF_BOXSCORES: "green",
    DataSet.BROOKS_PITCH_LOGS: "cyan",
    DataSet.BROOKS_PITCHFX: "magenta",
}

DATA_SET_FROM_NAME_MAP = {
    "Games for Date (bbref.com)": DataSet.BBREF_GAMES_FOR_DATE,
    "Games for Date (brooksbaseball.net)": DataSet.BROOKS_GAMES_FOR_DATE,
    "Boxscores (bbref.com)": DataSet.BBREF_BOXSCORES,
    "Pitch Logs for Game (brooksbaseball.net)": DataSet.BROOKS_PITCH_LOGS,
    "PitchFX Logs (brooksbaseball.net)": DataSet.BROOKS_PITCHFX,
}

DATA_SET_TO_NAME_MAP = {data_set: name for name, data_set in DATA_SET_FROM_NAME_MAP.items()}

CLI_COLORS = [
    "black",
    "red",
    "green",
    "yellow",
    "blue",
    "magenta",
    "cyan",
    "white",
    "bright_black",
    "bright_red",
    "bright_green",
    "bright_yellow",
    "bright_blue",
    "bright_magenta",
    "bright_cyan",
    "bright_white",
]

EMOJIS = {
    "HAND_POINTER": "👉",
    "BOOKMARK": "🔖",
    "FOLDER": "📁",
    "CHART": "📊",
    "BOOK": "📚",
    "TABBED_FILES": "🗂️",
    "TOOLS": "🛠️ ",
    "KNIFE": "🔪",
    "ROBOT": "🤖",
    "KNOBS": "🎛️",
    "BASKET": "🧺",
    "PACKAGE": "📦",
    "BAGS": "🛍️",
    "HONEY_POT": "🍯",
    "CALENDAR": "📅",
    "CLOCK": "🕚",
    "PAGER": "📟",
    "SHRUG": "🤷‍♂️",
    "CRYING": "😢",
    "WEARY": "😩",
    "UPSIDE_DOWN": "🙃",
    "CONFUSED": "😕",
    "FAMILY": "👨‍👩‍👦",
    "THUMBS_UP": "👍",
    "CLOUD": "🌧",
    "SPIRAL": "🌀",
    "DIZZY": "💫",
    "BOLT": "⚡",
    "BOMB": "💣",
    "FLASHLIGHT": "🔦",
    "MICROSCOPE": "🔬",
    "NEWSPAPER": "📰",
    "CAP": "🧢",
    "BASEBALL": "⚾",
    "SOFTBALL": "🥎",
    "HOTDOG": "🌭",
    "CIRCLE": "🔵",
    "GEAR": "⚙️ ",
    "BANG": "💥",
    "BLUE_DIAMOND": "🔹",
    "CHECK": "☑️",
    "ASTERISK": "✳️",
    "UP": "⬆️",
    "DOWN": "⬇️",
    "LEFT": "⬅️",
    "RIGHT": "➡️",
    "PASSED": "✅",
    "FAILED": "❌",
    "QUESTION": "❔",
    "COOL": "🆒",
    "BACK": "👈",
    "EXIT": "❎",
}

MENU_NUMBERS = {
    1: "1️⃣",
    2: "2️⃣",
    3: "3️⃣",
    4: "4️⃣",
    5: "5️⃣",
    6: "6️⃣",
    7: "7️⃣",
    8: "8️⃣",
    9: "9️⃣",
    10: "🔟",
}

FAKE_SPINNER = {"interval": 140, "frames": ["⚡", "⚡"]}

FIGLET_FONTS = [
    "3-d",
    "acrobatic",
    "banner3-D",
    "basic",
    "charact6",
    "contrast",
    "cyberlarge",
    "doom",
    "double",
    "ebbs_1__",
    "eftiwater",
    "epic",
    "fender",
    "graffiti",
    "home_pak",
    "isometric1",
    "jazmine",
    "kban",
    "larry3d",
    "lean",
    "lockergnome",
    "nancyj-fancy",
    "os2",
    "peaks",
    "rowancap",
    "sblood",
    "shimrod",
    "slant",
    "smscript",
    "space_op",
    "speed",
    "stellar",
    "ticksslant",
    "tinker-toy",
    "tubular",
    "usaflag",
]
