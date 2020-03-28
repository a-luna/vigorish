"""Populate mlb_season table with initial data."""
from datetime import date

from tqdm import tqdm

from vigorish.enums import SeasonType
from vigorish.models.season import Season
from vigorish.util.result import Result


def populate_seasons(session):
    """Populate mlb_season table with initial data."""
    result = __add_mlb_seasons(session)
    if result.failure:
        return result
    session.commit()
    return Result.Ok()


def __add_mlb_seasons(session):
    try:
        mlb_seasons = [
            Season(
                year=2016,
                start_date=date(2016, 4, 3),
                end_date=date(2016, 10, 2),
                asg_date=date(2016, 7, 12),
                season_type=SeasonType.REGULAR_SEASON,
            ),
            Season(
                year=2016,
                start_date=date(2016, 10, 3),
                end_date=date(2016, 11, 2),
                season_type=SeasonType.POST_SEASON,
            ),
            Season(
                year=2017,
                start_date=date(2017, 4, 2),
                end_date=date(2017, 10, 1),
                asg_date=date(2017, 7, 11),
                season_type=SeasonType.REGULAR_SEASON,
            ),
            Season(
                year=2017,
                start_date=date(2017, 10, 3),
                end_date=date(2017, 11, 1),
                season_type=SeasonType.POST_SEASON,
            ),
            Season(
                year=2018,
                start_date=date(2018, 3, 29),
                end_date=date(2018, 10, 1),
                asg_date=date(2018, 7, 17),
                season_type=SeasonType.REGULAR_SEASON,
            ),
            Season(
                year=2018,
                start_date=date(2018, 10, 2),
                end_date=date(2018, 10, 31),
                season_type=SeasonType.POST_SEASON,
            ),
            Season(
                year=2019,
                start_date=date(2019, 3, 28),
                end_date=date(2019, 9, 29),
                asg_date=date(2019, 7, 9),
                season_type=SeasonType.REGULAR_SEASON,
            ),
            Season(
                year=2019,
                start_date=date(2019, 10, 1),
                end_date=date(2019, 10, 30),
                season_type=SeasonType.POST_SEASON,
            ),
            Season(
                year=2020,
                start_date=date(2020, 3, 26),
                end_date=date(2020, 9, 27),
                asg_date=date(2020, 7, 14),
                season_type=SeasonType.REGULAR_SEASON,
            ),
            Season(
                year=2020,
                start_date=date(2020, 9, 29),
                end_date=date(2020, 10, 28),
                season_type=SeasonType.POST_SEASON,
            ),
        ]

        for season in tqdm(
            mlb_seasons,
            desc="Populating mlb_season table..",
            unit="row",
            mininterval=0.12,
            maxinterval=5,
            unit_scale=True,
        ):
            session.add(season)
        return Result.Ok()
    except Exception as e:
        error = "Error: {error}".format(error=repr(e))
        session.rollback()
        return Result.Fail(error)
