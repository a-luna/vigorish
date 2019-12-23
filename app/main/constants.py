"""Constant string and numeric values that are used by various modules."""

SEASON_TYPE_DICT = dict(pre="Preseason", reg="Regular Season", post="Postseason")

TRADE_TYPE_DICT = dict(
    draft="Amateur Draft",
    trade="Trade",
    sign="Free Agent Signing",
    purchase="Purchase,",
    release="Release",
    fa="Granted Free Agency",
)

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

DEF_POSITION_DICT = {
    "P": "Pitcher",
    "C": "Catcher",
    "1B": "First Base",
    "2B": "Second Base",
    "3B": "Third Base",
    "SS": "Short Stop",
    "RF": "Right Field",
    "CF": "Center Field",
    "LF": "Left Field",
}

MLB_DATA_SETS = [
    "bbref_games_for_date",
    "bbref_boxscore",
    "bbref_player",
    "brooks_games_for_date",
    "brooks_pitch_log",
    "brooks_pitchfx",
    "all"
]

PBAR_LEN_DICT = dict(
    find_games_for_date=33,
    bbref_games_for_date=34,
    bbref_boxscore=28,
    brooks_games_for_date=35,
    brooks_pitch_log=30,
    brooks_pitchfx=28)

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

AT_BAT_RESULTS_HIT = [
    "Double",
    "Home Run",
    "Single",
    "Triple",
]

AT_BAT_RESULTS_K = [
    "Strikeout",
    "Strikeout - DP",
]

AT_BAT_RESULTS_BB = [
    "Intent Walk",
    "Walk",
]

AT_BAT_RESULTS_HBP = [
    "Hit By Pitch",
]

AT_BAT_RESULTS_OUT = [
    "Bunt Groundout",
    "Bunt Lineout",
    "Bunt Pop Out",
    "Double Play",
    "Field Error",
    "Fielders Choice",
    "Fielders Choice Out",
    "Flyout",
    "Forceout",
    "Grounded Into DP",
    "Groundout",
    "Lineout",
    "Pop Out",
    "Runner Out",
    "Sac Bunt",
    "Sac Fly",
    "Sac Fly DP",
    "Strikeout",
    "Strikeout - DP",
]

AT_BAT_RESULTS_DP = [
    "Double Play",
    "Grounded Into DP",
    "Strikeout - DP",
]

PPB_PITCH_LOG_DICT = {
    "C":{
        "description": "called strike",
        "pitch_counts": 1
    },
    "S":{
        "description": "swinging strike",
        "pitch_counts": 1
    },
    "F":{
        "description": "foul",
        "pitch_counts": 1
    },
    "B":{
        "description": "ball",
        "pitch_counts": 1
    },
    "X":{
        "description": "ball put into play by batter",
        "pitch_counts": 1
    },
    "T":{
        "description": "foul tip",
        "pitch_counts": 1
    },
    "K":{
        "description": "strike (unknown type)",
        "pitch_counts": 1
    },
    "I":{
        "description": "intentional ball",
        "pitch_counts": 1
    },
    "H":{
        "description": "hit batter",
        "pitch_counts": 1
    },
    "L":{
        "description": "foul bunt",
        "pitch_counts": 1
    },
    "M":{
        "description": "missed bunt attempt",
        "pitch_counts": 1
    },
    "N":{
        "description": "no pitch (on balks and interference calls)",
        "pitch_counts": 0
    },
    "O":{
        "description": "foul tip on bunt",
        "pitch_counts": 1
    },
    "P":{
        "description": "pitchout",
        "pitch_counts": 1
    },
    "Q":{
        "description": "swinging on pitchout",
        "pitch_counts": 1
    },
    "R":{
        "description": "foul ball on pitchout",
        "pitch_counts": 1
    },
    "U":{
        "description": "unknown or missed pitch",
        "pitch_counts": 1
    },
    "V":{
        "description": "called ball because pitcher went to his mouth",
        "pitch_counts": 1
    },
    "Y":{
        "description": "ball put into play on pitchout",
        "pitch_counts": 1
    },
    "1":{
        "description": "(pickoff throw to first)",
        "pitch_counts": 0
    },
    "2":{
        "description": "(pickoff throw to second)",
        "pitch_counts": 0
    },
    "3":{
        "description": "(pickoff throw to third)",
        "pitch_counts": 0
    },
    ">":{
        "description": "(runner going on the pitch)",
        "pitch_counts": 0
    },
    "+":{
        "description": "(pickoff throw by the catcher)",
        "pitch_counts": 0
    },
    "*":{
        "description": "(the following pitch was blocked by the catcher)",
        "pitch_counts": 0
    },
    ".":{
        "description": "(play not involving the batter)",
        "pitch_counts": 0
    },
}

VENUE_TERMS = ["stadium", "park", "field", "coliseum", "centre", "estadio", "dome"]

BROOKS_DASHBOARD_DATE_FORMAT = "%m/%d/%Y"
T_BROOKS_DASH_URL = "http://www.brooksbaseball.net/dashboard.php?dts=${date}"
T_BBREF_DASH_URL = (
    "https://www.baseball-reference.com/boxes/?month=${m}&day=${d}&year=${y}"
)

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
    "bright_white"
]
