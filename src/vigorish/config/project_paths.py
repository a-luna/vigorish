from pathlib import Path


APP_FOLDER = Path(__file__).parent.parent
VIG_FOLDER = Path.home().joinpath(".vig")

NIGHTMAREJS_FOLDER = APP_FOLDER.joinpath("nightmarejs")
NODEJS_INBOX = NIGHTMAREJS_FOLDER.joinpath("inbox")
NODEJS_OUTBOX = NIGHTMAREJS_FOLDER.joinpath("outbox")
NODEJS_SCRIPT = NIGHTMAREJS_FOLDER.joinpath("scrape_job.js")

CSV_FOLDER = APP_FOLDER.joinpath("setup/csv")
PEOPLE_CSV = CSV_FOLDER.joinpath("People.csv")
TEAM_CSV = CSV_FOLDER.joinpath("Teams.csv")
PLAYER_ID_MAP_CSV = CSV_FOLDER.joinpath("bbref_player_id_map.csv")
PLAYER_TEAM_MAP_CSV = CSV_FOLDER.joinpath("bbref_player_team_map.csv")

ROOT_FOLDER = APP_FOLDER.parent.parent
TEST_CSV_FOLDER = ROOT_FOLDER.joinpath("tests/csv")
TEST_PEOPLE_CSV = TEST_CSV_FOLDER.joinpath("People.csv")
TEST_TEAM_CSV = TEST_CSV_FOLDER.joinpath("Teams.csv")
TEST_PLAYER_ID_MAP_CSV = TEST_CSV_FOLDER.joinpath("bbref_player_id_map.csv")
