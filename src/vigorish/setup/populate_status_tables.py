from tqdm import tqdm

from vigorish.models.season import Season
from vigorish.models.status_date import DateScrapeStatus
from vigorish.util.result import Result


def populate_status_tables(db_session):
    try:
        mlb_seasons = Season.all_regular_seasons(db_session)
        with tqdm(
            total=len(mlb_seasons),
            unit="season",
            mininterval=0.12,
            maxinterval=5,
            position=0,
            ncols=90,
        ) as pbar:
            for season in mlb_seasons:
                pbar.set_description("Populating status_date table.")
                for game_date in season.get_date_range():
                    scrape_status = DateScrapeStatus(game_date=game_date, season_id=season.id)
                    db_session.add(scrape_status)
                pbar.update()
        db_session.commit()
        return Result.Ok()
    except Exception as e:
        error = "Error: {error}".format(error=repr(e))
        db_session.rollback()
        return Result.Fail(error)
