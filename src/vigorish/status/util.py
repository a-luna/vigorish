import vigorish.database as db
from vigorish.tasks.scrape_mlb_player_info import ScrapeMlbPlayerInfoTask
from vigorish.util.dt_format_strings import DATE_ONLY_2
from vigorish.util.result import Result
from vigorish.util.string_helpers import validate_bbref_game_id


def get_date_status_from_bbref_game_id(db_session, bbref_game_id):
    game_dict = validate_bbref_game_id(bbref_game_id).value
    return get_date_status(db_session, game_dict["game_date"])


def get_date_status(db_session, game_date):
    date_status = db.DateScrapeStatus.find_by_date(db_session, game_date)
    if date_status:
        return Result.Ok(date_status)
    error = f"scrape_status_date does not contain an entry for game_date: {game_date.strftime(DATE_ONLY_2)}"
    return Result.Fail(error)


def get_game_status(db_session, game_date, bbref_game_id, bb_game_id):
    result = get_game_status_record(db_session, bbref_game_id, bb_game_id)
    if result.failure:
        game_status = create_game_status_record(
            db_session, game_date, bbref_game_id=bbref_game_id, bb_game_id=bb_game_id
        )
        db_session.add(game_status)
    else:
        game_status = result.value
    return game_status


def get_player_id(db_session, mlb_id=None, bbref_id=None, boxscore=None):
    if mlb_id:
        player_id = db.PlayerId.find_by_mlb_id(db_session, mlb_id)
    if bbref_id:
        player_id = db.PlayerId.find_by_bbref_id(db_session, bbref_id)
    if not player_id and not boxscore:
        raise ValueError(f"Failed to retrive player id info! (bbref_id: {bbref_id})")
    if not player_id:  # pragma: no cover
        name_with_team = [k for k, v in boxscore.player_name_dict.items() if v == bbref_id][0]
        name = name_with_team.split(",")[0]
        scrape_task = ScrapeMlbPlayerInfoTask(get_mock_app(db_session))
        result = scrape_task.execute(name, bbref_id, boxscore.game_date)
        if result.failure:
            raise ValueError(f"Failed to retrive or scrape player id info! (bbref_id: {bbref_id})")
        player = result.value
        player_id = db.PlayerId.find_by_mlb_id(db_session, player.mlb_id)
    if not player_id:
        raise ValueError(f"Failed to retrive or scrape player id info! (bbref_id: {bbref_id})")
    return player_id


def get_mock_app(db_session):
    class MockApp:
        def __init__(self, db_session):
            self.dotenv = ""
            self.config = ""
            self.db_engine = ""
            self.db_session = db_session
            self.scraped_data = ""

    return MockApp(db_session)


def get_pitch_app_status(db_session, bbref_game_id, bb_game_id, game_status, player_id, pitch_app_id):
    result = get_pitch_app_status_record(db_session, pitch_app_id)
    if result.failure:
        pitch_app_status = create_pitch_app_status_record(
            bbref_game_id, bb_game_id, game_status, player_id, pitch_app_id
        )
        db_session.add(pitch_app_status)
    else:
        pitch_app_status = result.value
    return pitch_app_status


def get_game_status_record(db_session, bbref_game_id, bb_game_id):
    game_status = db.GameScrapeStatus.find_by_bbref_game_id(db_session, bbref_game_id)
    if game_status:
        return Result.Ok(game_status)
    game_status = db.GameScrapeStatus.find_by_bb_game_id(db_session, bb_game_id)
    if game_status:
        game_status.bbref_game_id = bbref_game_id
        return Result.Ok(game_status)
    error = f"scrape_status_game does not contain an entry for game_id: {bbref_game_id}"
    return Result.Fail(error)


def get_pitch_app_status_record(db_session, pitch_app_id):
    pitch_app_status = db.PitchAppScrapeStatus.find_by_pitch_app_id(db_session, pitch_app_id)
    if pitch_app_status:
        return Result.Ok(pitch_app_status)
    error = f"scrape_status_pitch_app does not contain an entry for pitch_app_id: {pitch_app_id}"
    return Result.Fail(error)


def create_game_status_record(db_session, game_date, bbref_game_id, bb_game_id):
    date_status = db.DateScrapeStatus.find_by_date(db_session, game_date)
    game_status = db.GameScrapeStatus()
    game_status.game_date = game_date
    game_status.bbref_game_id = bbref_game_id
    if bb_game_id:
        game_status.bb_game_id = bb_game_id
    game_status.scrape_status_date_id = date_status.id
    game_status.season_id = date_status.season_id
    return game_status


def create_pitch_app_status_record(bbref_game_id, bb_game_id, game_status, player_id, pitch_app_id):
    pitch_app_status = db.PitchAppScrapeStatus()
    pitch_app_status.pitcher_id_mlb = player_id.mlb_id
    pitch_app_status.pitch_app_id = pitch_app_id
    pitch_app_status.bbref_game_id = bbref_game_id
    pitch_app_status.bb_game_id = bb_game_id
    pitch_app_status.scrape_status_game_id = game_status.id
    pitch_app_status.scrape_status_date_id = game_status.date.id
    pitch_app_status.pitcher_id = player_id.db_player_id
    pitch_app_status.season_id = game_status.season.id
    return pitch_app_status
