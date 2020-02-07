from tqdm import tqdm

from app.main.constants import SEASON_TYPE_DICT
from app.main.models.season import Season
from app.main.models.status_date import DateScrapeStatus
from app.main.util.result import Result


def populate_status_tables(session):
    try:
        mlb_seasons = Season.all_regular_seasons(session)
        with tqdm(
            total=len(mlb_seasons),
            unit="season",
            mininterval=0.12,
            maxinterval=5,
            position=0,
        ) as pbar:
            for season in mlb_seasons:
                pbar.set_description(f"Populating status_date table.")
                for game_date in season.get_date_range():
                    scrape_status = DateScrapeStatus(game_date, season.id)
                    session.add(scrape_status)
                pbar.update()
        session.commit()
        return Result.Ok()
    except Exception as e:
        error = "Error: {error}".format(error=repr(e))
        session.rollback()
        return Result.Fail(error)
