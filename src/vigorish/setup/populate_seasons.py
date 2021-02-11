"""Populate mlb_season table with initial data."""
from datetime import date

from tqdm import tqdm

import vigorish.database as db
from vigorish.enums import SeasonType
from vigorish.util.result import Result


def populate_seasons(db_session):
    """Populate mlb_season table with initial data."""
    result = add_mlb_seasons(db_session)
    if result.failure:
        return result
    db_session.commit()
    return Result.Ok()


def add_mlb_seasons(db_session):
    try:
        mlb_seasons = [
            db.Season(
                year=2014,
                start_date=date(2014, 3, 22),
                end_date=date(2014, 9, 28),
                asg_date=date(2014, 7, 14),
                season_type=SeasonType.REGULAR_SEASON,
            ),
            db.Season(
                year=2014,
                start_date=date(2014, 9, 30),
                end_date=date(2014, 10, 29),
                season_type=SeasonType.POST_SEASON,
            ),
            db.Season(
                year=2015,
                start_date=date(2015, 4, 5),
                end_date=date(2015, 10, 4),
                asg_date=date(2015, 7, 14),
                season_type=SeasonType.REGULAR_SEASON,
            ),
            db.Season(
                year=2015,
                start_date=date(2015, 10, 6),
                end_date=date(2015, 11, 1),
                season_type=SeasonType.POST_SEASON,
            ),
            db.Season(
                year=2016,
                start_date=date(2016, 4, 3),
                end_date=date(2016, 10, 2),
                asg_date=date(2016, 7, 12),
                season_type=SeasonType.REGULAR_SEASON,
            ),
            db.Season(
                year=2016,
                start_date=date(2016, 10, 3),
                end_date=date(2016, 11, 2),
                season_type=SeasonType.POST_SEASON,
            ),
            db.Season(
                year=2017,
                start_date=date(2017, 4, 2),
                end_date=date(2017, 10, 1),
                asg_date=date(2017, 7, 11),
                season_type=SeasonType.REGULAR_SEASON,
            ),
            db.Season(
                year=2017,
                start_date=date(2017, 10, 3),
                end_date=date(2017, 11, 1),
                season_type=SeasonType.POST_SEASON,
            ),
            db.Season(
                year=2018,
                start_date=date(2018, 3, 29),
                end_date=date(2018, 10, 1),
                asg_date=date(2018, 7, 17),
                season_type=SeasonType.REGULAR_SEASON,
            ),
            db.Season(
                year=2018,
                start_date=date(2018, 10, 2),
                end_date=date(2018, 10, 31),
                season_type=SeasonType.POST_SEASON,
            ),
            db.Season(
                year=2019,
                start_date=date(2019, 3, 28),
                end_date=date(2019, 9, 29),
                asg_date=date(2019, 7, 9),
                season_type=SeasonType.REGULAR_SEASON,
            ),
            db.Season(
                year=2019,
                start_date=date(2019, 10, 1),
                end_date=date(2019, 10, 30),
                season_type=SeasonType.POST_SEASON,
            ),
            db.Season(
                year=2020,
                start_date=date(2020, 7, 23),
                end_date=date(2020, 9, 27),
                season_type=SeasonType.REGULAR_SEASON,
            ),
            db.Season(
                year=2020,
                start_date=date(2020, 9, 29),
                end_date=date(2020, 10, 31),
                season_type=SeasonType.POST_SEASON,
            ),
        ]

        for season in tqdm(
            mlb_seasons,
            desc="Populating mlb_season table....",
            unit="row",
            mininterval=0.12,
            maxinterval=5,
            unit_scale=True,
            ncols=90,
        ):
            db_session.add(season)
        return Result.Ok()
    except Exception as e:
        error = f"Error: {repr(e)}"
        db_session.rollback()
        return Result.Fail(error)
