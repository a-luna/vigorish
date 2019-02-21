"""Constant string and numeric values that are used by various modules."""

SEASON_TYPE_DICT = dict(
    pre='Preseason',
    reg='Regular Season',
    post='Postseason'
)

TRADE_TYPE_DICT = dict(
    draft='Amateur Draft',
    trade='Trade',
    sign='Free Agent Signing',
    purchase='Purchase,',
    release='Release',
    fa='Granted Free Agency'
)

MLB_DATA_SETS = [
    'bbref_games_for_date',
    'bbref_boxscores',
    'bbref_players',
    'brooks_games_for_date',
    'brooks_pitch_logs',
    'brooks_pitchfx'
]

BROOKS_DASHBOARD_DATE_FORMAT = '%m/%d/%Y'
T_BROOKS_DASH_URL = 'http://www.brooksbaseball.net/dashboard.php?dts=${date}'
