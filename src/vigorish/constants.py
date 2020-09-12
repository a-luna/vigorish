"""Constant values that are referenced from multiple places."""
from bullet import colors

from vigorish.enums import DataSet, VigFile

ENV_VAR_NAMES = [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_DEFAULT_REGION",
    "CONFIG_FILE",
    "DATABASE_URL",
]

TEAM_ID_DICT = {
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

PITCH_TYPE_DICT = dict(
    CH="Changeup",
    CU="Curveball",
    EP="Eephus",
    FA="Fastball",
    FC="Cutter",
    FF="Four-seam Fastball",
    FS="Splitter",
    FT="Two-seam Fastball",
    FO="Forkball",
    IN="Intent ball",
    KC="Knuckle ball Curve",
    KN="Knuckle ball",
    PO="Pitch Out",
    SC="Screwball",
    SI="Sinker",
    SL="Slider",
    UN="Unknown",
)

AT_BAT_RESULTS_ALL = [
    "Bunt Groundout",
    "Bunt Lineout",
    "Bunt Pop Out",
    "Double",
    "Double Play",
    "Field Error",
    "Fielders Choice",
    "Fielders Choice Out",
    "Flyout",
    "Forceout",
    "Grounded Into DP",
    "Groundout",
    "Hit By Pitch",
    "Home Run",
    "Intent Walk",
    "Lineout",
    "Pop Out",
    "Runner Out",
    "Sac Bunt",
    "Sac Fly",
    "Sac Fly DP",
    "Single",
    "Strikeout",
    "Strikeout - DP",
    "Triple",
    "Walk",
]

PPB_PITCH_LOG_DICT = {
    "C": {"description": "Called strike", "pitch_counts": 1},
    "S": {"description": "Swinging strike", "pitch_counts": 1},
    "F": {"description": "Foul", "pitch_counts": 1},
    "B": {"description": "Ball", "pitch_counts": 1},
    "X": {"description": "Ball put into play by batter", "pitch_counts": 1},
    "T": {"description": "Foul tip", "pitch_counts": 1},
    "K": {"description": "Strike (unknown type)", "pitch_counts": 1},
    "I": {"description": "Intentional ball", "pitch_counts": 1},
    "H": {"description": "Hit batter", "pitch_counts": 1},
    "L": {"description": "Foul bunt", "pitch_counts": 1},
    "M": {"description": "Missed bunt attempt", "pitch_counts": 1},
    "N": {"description": "No pitch (on balks and interference calls)", "pitch_counts": 0},
    "O": {"description": "Foul tip on bunt", "pitch_counts": 1},
    "P": {"description": "Pitchout", "pitch_counts": 1},
    "Q": {"description": "Swinging on pitchout", "pitch_counts": 1},
    "R": {"description": "Foul ball on pitchout", "pitch_counts": 1},
    "U": {"description": "Unknown or missed pitch", "pitch_counts": 1},
    "V": {"description": "(Called ball because pitcher went to his mouth)", "pitch_counts": 0},
    "Y": {"description": "Ball put into play on pitchout", "pitch_counts": 1},
    "1": {"description": "(Pickoff throw to first)", "pitch_counts": 0},
    "2": {"description": "(Pickoff throw to second)", "pitch_counts": 0},
    "3": {"description": "(Pickoff throw to third)", "pitch_counts": 0},
    ">": {"description": "(Runner going on the pitch)", "pitch_counts": 0},
    "+": {"description": "(Pickoff throw by the catcher)", "pitch_counts": 0},
    "*": {"description": "(The following pitch was blocked by the catcher)", "pitch_counts": 0},
    ".": {"description": "(Play not involving the batter)", "pitch_counts": 0},
}

VENUE_TERMS = ["stadium", "park", "field", "coliseum", "centre", "estadio", "dome"]

JOB_SPINNER_COLORS = {
    DataSet.BBREF_GAMES_FOR_DATE: "red",
    DataSet.BROOKS_GAMES_FOR_DATE: "blue",
    DataSet.BBREF_BOXSCORES: "green",
    DataSet.BROOKS_PITCH_LOGS: "cyan",
    DataSet.BROOKS_PITCHFX: "magenta",
}

DATA_SET_NAMES_LONG = {
    "Games for Date (bbref.com)": DataSet.BBREF_GAMES_FOR_DATE,
    "Games for Date (brooksbaseball.net)": DataSet.BROOKS_GAMES_FOR_DATE,
    "Boxscores (bbref.com)": DataSet.BBREF_BOXSCORES,
    "Pitch Logs for Game (brooksbaseball.net)": DataSet.BROOKS_PITCH_LOGS,
    "PitchFX Logs (brooksbaseball.net)": DataSet.BROOKS_PITCHFX,
}

FILE_TYPE_NAME_MAP = {str(ft): ft for ft in VigFile}
DATA_SET_NAME_MAP = {str(ds): ds for ds in DataSet}

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

BULLET_COLORS = {
    "black": colors.foreground["black"],
    "red": colors.foreground["red"],
    "green": colors.foreground["green"],
    "yellow": colors.foreground["yellow"],
    "blue": colors.foreground["blue"],
    "magenta": colors.foreground["magenta"],
    "cyan": colors.foreground["cyan"],
    "white": colors.foreground["white"],
    "bright_black": colors.bright(colors.foreground["black"]),
    "bright_red": colors.bright(colors.foreground["red"]),
    "bright_green": colors.bright(colors.foreground["green"]),
    "bright_yellow": colors.bright(colors.foreground["yellow"]),
    "bright_blue": colors.bright(colors.foreground["blue"]),
    "bright_magenta": colors.bright(colors.foreground["magenta"]),
    "bright_cyan": colors.bright(colors.foreground["cyan"]),
    "bright_white": colors.bright(colors.foreground["white"]),
}

EMOJI_DICT = dict(
    HAND_POINTER="👉",
    BOOKMARK="🔖",
    FOLDER="📁",
    CHART="📊",
    BOOK="📚",
    TABBED_FILES="🗂️",
    TOOLS="🛠️ ",
    KNIFE="🔪",
    ROBOT="🤖",
    KNOBS="🎛️",
    BASKET="🧺",
    PACKAGE="📦",
    BAGS="🛍️",
    HONEY_POT="🍯",
    CLOCK="🕚",
    PAGER="📟",
    SHRUG="🤷‍♂️",
    CRYING="😢",
    WEARY="😩",
    UPSIDE_DOWN="🙃",
    CONFUSED="😕",
    THUMBS_UP="👍",
    CLOUD="🌧",
    SPIRAL="🌀",
    DIZZY="💫",
    BOLT="⚡",
    BOMB="💣",
    FLASHLIGHT="🔦",
    CIRCLE="🔵",
    GEAR="⚙️ ",
    BANG="💥",
    BLUE_DIAMOND="🔹",
    CHECK="☑️",
    ASTERISK="✳️",
    PASSED="✅",
    FAILED="❌",
    QUESTION="❔",
    COOL="🆒",
    BACK="👈",
    EXIT="❎",
)

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
