"""Constant values that are referenced from multiple places."""

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
    TOOLS="🛠️",
    KNIFE="🔪",
    KNOBS="🎛️",
    BASKET="🧺",
    BAGS="🛍️",
    CLOCK="🕚",
    SPIRAL="🌀",
    CIRCLE="🔵",
    DIAMOND="🔹",
    ONE="1️⃣",
    TWO="2️⃣",
    THREE="3️⃣",
    FOUR="4️⃣",
    FIVE="5️⃣",
    QUESTION="❔",
    BACK="⬅",
    EXIT="❌",
)

MENU_NUMBERS = {1: "1️⃣", 2: "2️⃣", 3: "3️⃣", 4: "4️⃣", 5: "5️⃣"}

CONFIG_EMOJI_DICT = dict(
    SCRAPE_TOOL=EMOJI_DICT.get("KNIFE", ""),
    SCRAPE_CONDITION=EMOJI_DICT.get("QUESTION", ""),
    STATUS_REPORT=EMOJI_DICT.get("CHART", ""),
    URL_SCRAPE_DELAY=EMOJI_DICT.get("CLOCK", ""),
    BATCH_JOB_SETTINGS=EMOJI_DICT.get("BAGS", ""),
    BATCH_SCRAPE_DELAY=EMOJI_DICT.get("CLOCK", ""),
    S3_BUCKET=EMOJI_DICT.get("BASKET", ""),
    HTML_STORAGE=EMOJI_DICT.get("QUESTION", ""),
    HTML_LOCAL_FOLDER_PATH=EMOJI_DICT.get("FOLDER", ""),
    HTML_S3_FOLDER_PATH=EMOJI_DICT.get("BOOKMARK", ""),
    JSON_STORAGE=EMOJI_DICT.get("QUESTION", ""),
    JSON_LOCAL_FOLDER_PATH=EMOJI_DICT.get("FOLDER", ""),
    JSON_S3_FOLDER_PATH=EMOJI_DICT.get("BOOKMARK", ""),
)
