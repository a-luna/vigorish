from app.main.tasks.bbref_daily_boxscores import ScrapeBBRefDailyBoxscores
from app.main.tasks.bbref_daily_games import ScrapeBBRefDailyGames
from app.main.tasks.bbref_player_bio import ScrapeBBrefPlayerBio
from app.main.tasks.brooks_daily_games import ScrapeBrooksDailyGames
from app.main.tasks.brooks_daily_pitch_logs import ScrapeBrooksDailyPitchLogs
from app.main.tasks.brooks_daily_pitchfx import ScrapeBrooksDailyPitchFxLogs
from app.main.tasks.find_games_for_date import FindAllGamesForDateTask
from app.main.util.result import Result


TASK_LIST_MENU = dict(
    bbref_games_for_date=[ScrapeBBRefDailyGames],
    bbref_boxscores=[ScrapeBBRefDailyBoxscores],
    bbref_player=[ScrapeBBrefPlayerBio],
    brooks_games_for_date=[ScrapeBrooksDailyGames],
    brooks_pitch_logs=[ScrapeBrooksDailyPitchLogs],
    brooks_pitchfx=[ScrapeBrooksDailyPitchFxLogs],
    all=[
        ScrapeBBRefDailyGames,
        ScrapeBrooksDailyGames,
        ScrapeBBRefDailyBoxscores,
        ScrapeBrooksDailyPitchLogs,
        ScrapeBrooksDailyPitchFxLogs,
    ],
)


def get_task_list(data_set):
    scrape_tasks = TASK_LIST_MENU.get(data_set, None)
    if not scrape_tasks:
        error = f'Invalid value for data-set: "{data_set}"'
        return Result.Fail(error)
    return Result.Ok(scrape_tasks)
