from pathlib import Path

APP_FOLDER = Path(__file__).parent.parent
ROOT_FOLDER = APP_FOLDER.parent.parent
VIG_FOLDER = Path.home().joinpath(".vig")
DOTENV_FILE = VIG_FOLDER.joinpath(".env")
CONFIG_FILE = VIG_FOLDER.joinpath("vig.config.json")

SQLITE_DEV_URL = f"sqlite:///{VIG_FOLDER.joinpath('vig_dev.db')}"
SQLITE_PROD_URL = f"sqlite:///{VIG_FOLDER.joinpath('vig.db')}"

NIGHTMAREJS_FOLDER = APP_FOLDER.joinpath("nightmarejs")
NODEJS_INBOX = NIGHTMAREJS_FOLDER.joinpath("inbox")
NODEJS_OUTBOX = NIGHTMAREJS_FOLDER.joinpath("outbox")
NODEJS_SCRIPT = NIGHTMAREJS_FOLDER.joinpath("scrape_job.js")

JSON_FOLDER = APP_FOLDER.joinpath("setup/json")
CSV_FOLDER = APP_FOLDER.joinpath("setup/csv")
PEOPLE_CSV = CSV_FOLDER.joinpath("People.csv")
TEAM_CSV = CSV_FOLDER.joinpath("Teams.csv")
PLAYER_ID_MAP_CSV = CSV_FOLDER.joinpath("bbref_player_id_map.csv")
PLAYER_TEAM_MAP_CSV = CSV_FOLDER.joinpath("bbref_player_team_map.csv")
