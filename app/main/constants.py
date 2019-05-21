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
