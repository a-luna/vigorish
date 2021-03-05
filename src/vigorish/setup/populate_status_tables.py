from tqdm import tqdm

import vigorish.database as db
from vigorish.util.result import Result


def populate_status_tables(db_session):
    try:
        mlb_seasons = db.Season.get_all_regular_seasons(db_session)
        with tqdm(
            total=len(mlb_seasons),
            unit="season",
            mininterval=0.12,
            maxinterval=5,
            position=0,
            ncols=90,
        ) as pbar:
            for season in mlb_seasons:
                pbar.set_description("Populating status_date table...")
                for game_date in season.get_date_range():
                    scrape_status = db.DateScrapeStatus(game_date=game_date, season_id=season.id)
                    db_session.add(scrape_status)
                pbar.update()
        db_session.commit()
        return Result.Ok()
    except Exception as e:
        error = f"Error: {repr(e)}"
        db_session.rollback()
        return Result.Fail(error)
