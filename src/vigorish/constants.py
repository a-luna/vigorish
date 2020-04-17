"""Constant values that are referenced from multiple places."""
from vigorish.enums import DataSet

DEFAULT_CONFIG_SETTINGS = {
    "STATUS_REPORT": {
        "CONFIG_TYPE": "Enum",
        "ENUM_NAME": "StatusReport",
        "DESCRIPTION": (
            "After a scrape job has successfully completed, you can display a report for the MLB "
            "season displaying various metrics for all data sets. The options below determine the "
            "level of detail reported."
        ),
        "SAME_SETTING_FOR_ALL_DATA_SETS": True,
    },
    "S3_BUCKET": {
        "CONFIG_TYPE": "str",
        "DESCRIPTION": "The name of the S3 bucket to use for storing HTML and/or JSON files",
        "SAME_SETTING_FOR_ALL_DATA_SETS": True,
    },
    "SCRAPE_CONDITION": {
        "CONFIG_TYPE": "Enum",
        "ENUM_NAME": "ScrapeCondition",
        "DESCRIPTION": (
            "By default, HTML is scraped and parsed only once (ONLY_MISSING_DATA). You can "
            "overwrite existing data by selecting ALWAYS, or prevent any data from being scraped "
            "by selecting NEVER."
        ),
        "SAME_SETTING_FOR_ALL_DATA_SETS": True,
    },
    "URL_SCRAPE_DELAY": {
        "CONFIG_TYPE": "Numeric",
        "CLASS_NAME": "UrlScrapeDelay",
        "DESCRIPTION": (
            "As a common courtesy (a.k.a, to avoid being banned), after scraping a webpage you "
            "should wait for a few seconds before requesting the next URL. You can specify a "
            "single length of time to use for all URLs, or create random delay lengths by "
            "specifying a minimum and maximum length of time."
        ),
        "SAME_SETTING_FOR_ALL_DATA_SETS": True,
    },
    "BATCH_JOB_SETTINGS": {
        "CONFIG_TYPE": "Numeric",
        "CLASS_NAME": "BatchJobSettings",
        "DESCRIPTION": (
            "Number of URLs to scrape per batch. You can specify a single amount to use for all "
            "batches, or create random batch sizes by specifying a minimum and maximum batch size."
        ),
        "SAME_SETTING_FOR_ALL_DATA_SETS": True,
    },
    "BATCH_SCRAPE_DELAY": {
        "CONFIG_TYPE": "Numeric",
        "CLASS_NAME": "BatchScrapeDelay",
        "DESCRIPTION": (
            "Some websites will ban you even if you wait for a few seconds between each request. "
            "To avoid being banned, you can scrape URLs in batches (recommended batch size: ~50 "
            "URLs/batch) and wait for a long period of time (30-45 minutes) before you begin a "
            "new batch. You can specify a single length of time to use for all batches or create "
            "random delay lengths by specifying a minimum and maximum length of time."
        ),
        "SAME_SETTING_FOR_ALL_DATA_SETS": False,
    },
    "HTML_STORAGE": {
        "CONFIG_TYPE": "Enum",
        "ENUM_NAME": "HtmlStorageOption",
        "DESCRIPTION": (
            "By default, HTML is NOT saved after it has been parsed. However, you can choose to "
            "save scraped HTML in a local folder, an S3 bucket, or both."
        ),
        "SAME_SETTING_FOR_ALL_DATA_SETS": True,
    },
    "HTML_LOCAL_FOLDER_PATH": {
        "CONFIG_TYPE": "Path",
        "CLASS_NAME": "LocalFolderPathSetting",
        "DESCRIPTION": (
            "Local folder path where scraped HTML should be stored. The application will always "
            "check for saved HTML content in this location before sending a request to the "
            "website."
        ),
        "SAME_SETTING_FOR_ALL_DATA_SETS": True,
    },
    "HTML_S3_FOLDER_PATH": {
        "CONFIG_TYPE": "Path",
        "CLASS_NAME": "S3FolderPathSetting",
        "DESCRIPTION": (
            "Path to a folder within an S3 bucket where scraped HTML should be stored. The "
            "application will always check for saved HTML content in this location before sending "
            "a request to the website."
        ),
        "SAME_SETTING_FOR_ALL_DATA_SETS": True,
    },
    "JSON_STORAGE": {
        "CONFIG_TYPE": "Enum",
        "ENUM_NAME": "JsonStorageOption",
        "DESCRIPTION": (
            "MLB data is parsed from HTML and stored in JSON docs. You can store the JSON docs "
            "in a local folder, an S3 bucket, or both."
        ),
        "SAME_SETTING_FOR_ALL_DATA_SETS": True,
    },
    "JSON_LOCAL_FOLDER_PATH": {
        "CONFIG_TYPE": "Path",
        "CLASS_NAME": "LocalFolderPathSetting",
        "DESCRIPTION": "Local folder path where parsed JSON data should be stored.",
        "SAME_SETTING_FOR_ALL_DATA_SETS": True,
    },
    "JSON_S3_FOLDER_PATH": {
        "CONFIG_TYPE": "Path",
        "CLASS_NAME": "S3FolderPathSetting",
        "DESCRIPTION": (
            "Path to a folder within an S3 bucket where parsed JSON data should be stored."
        ),
        "SAME_SETTING_FOR_ALL_DATA_SETS": True,
    },
}

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

DATA_SET_CHOICES = {
    "all": DataSet.ALL,
    "bbref_games": DataSet.BBREF_GAMES_FOR_DATE,
    "brooks_games": DataSet.BROOKS_GAMES_FOR_DATE,
    "bbref_boxscores": DataSet.BBREF_BOXSCORES,
    "brooks_pitch_logs": DataSet.BROOKS_PITCH_LOGS,
    "brooks_pitchfx": DataSet.BROOKS_PITCHFX,
}

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
    BAGS="🛍️",
    CLOCK="🕚",
    SHRUG="🤷‍♂️",
    CRYING="😢",
    WEARY="😩",
    THUMBS_UP="👍",
    CLOUD="🌧",
    SPIRAL="🌀",
    DIZZY="💫",
    BOMB="💣",
    CIRCLE="🔵",
    GEAR="⚙️ ",
    BLUE_DIAMOND="🔹",
    CHECK="☑️",
    ASTERISK="✳️",
    PASSED="✅",
    FAILED="🚫",
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
