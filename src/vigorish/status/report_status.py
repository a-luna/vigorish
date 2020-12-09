import os

from vigorish.cli.components.dict_viewer import DictListTableViewer
from vigorish.cli.components.models import DisplayPage
from vigorish.cli.components.page_viewer import PageViewer
from vigorish.database import DateScrapeStatus, Season
from vigorish.enums import StatusReport
from vigorish.util.datetime_util import get_date_range
from vigorish.util.dt_format_strings import DATE_MONTH_NAME, DATE_ONLY
from vigorish.util.list_helpers import flatten_list2d
from vigorish.util.result import Result


def report_status_single_date(db_session, game_date, report_type):
    pages = []
    result = validate_single_date(db_session, game_date)
    if result.failure:
        return result
    date_status = result.value
    date_str = game_date.strftime(DATE_MONTH_NAME)
    heading = f"### OVERALL STATUS FOR {date_str} ###"
    pages.append(DisplayPage(date_status.status_report(), heading))
    if report_type == StatusReport.SINGLE_DATE_WITH_GAME_STATUS:
        game_status_dict = date_status.games_status_report()
        for num, (game_id, game_report) in enumerate(game_status_dict.items(), start=1):
            heading = f"### STATUS FOR {game_id} (Game {num}/{len(game_status_dict)}) ###"
            pages.append(DisplayPage(game_report, heading))
    if (
        report_type == StatusReport.DATE_DETAIL_MISSING_PITCHFX
        or report_type == StatusReport.SINGLE_DATE_WITH_GAME_STATUS
    ):
        heading = f"### MISSING PITCHFX DATA FOR {date_str} ###"
        missing_ids_str = _get_missing_pfx_ids_for_date(db_session, date_status)
        pages.append(DisplayPage(missing_ids_str, heading))
    date_report = PageViewer(
        pages,
        prompt="Press Enter to return to the Main Menu",
        confirm_only=True,
        heading_color="bright_magenta",
        text_color="bright_magenta",
        wrap_text=False,
    )
    if os.environ.get("ENV") == "TEST":
        for page in pages:
            page.display(
                heading_color="bright_magenta",
                text_color="bright_magenta",
                wrap_text=False,
            )
    return Result.Ok(date_report)


def validate_single_date(db_session, game_date):
    season = Season.find_by_year(db_session, game_date.year)
    date_is_valid = Season.is_date_in_season(db_session, game_date).success
    date_str = game_date.strftime(DATE_ONLY)
    if not date_is_valid:
        error = (
            f"'{date_str}' is not within the {season.name}:\n"
            f"season_start_date: {season.start_date_str}\n"
            f"season_end_date: {season.end_date_str}"
        )
        return Result.Fail(error)
    date_status = DateScrapeStatus.find_by_date(db_session, game_date)
    if not date_status:
        error = f"scrape_status_date does not contain an entry for date: {date_str}"
        return Result.Fail(error)
    return Result.Ok(date_status)


def report_season_status(db_session, year, report_type):
    if report_type == StatusReport.NONE:
        return Result.Fail("No_report")
    season = Season.find_by_year(db_session, year)
    if report_type == StatusReport.SEASON_SUMMARY:
        heading = f"### STATUS REPORT FOR {season.name} ###"
        pages = [DisplayPage(season.status_report(), heading)]
        date_report = PageViewer(
            pages,
            prompt="Press Enter to return to the Main Menu",
            confirm_only=True,
            heading_color="bright_yellow",
            text_color="bright_yellow",
            wrap_text=False,
        )
        if os.environ.get("ENV") == "TEST":
            for page in pages:
                page.display(
                    heading_color="bright_cyan",
                    text_color="bright_cyan",
                    wrap_text=False,
                )
        return Result.Ok(date_report)
    start_date = season.start_date
    end_date = season.end_date
    return report_date_range_status(db_session, start_date, end_date, report_type)


def report_date_range_status(db_session, start_date, end_date, report_type):
    if report_type == StatusReport.NONE:
        return Result.Fail("No_report")
    result = construct_date_range_status(db_session, start_date, end_date, report_type)
    if result.failure:
        return result
    status_date_range = result.value
    return display_date_range_status(
        db_session, start_date, end_date, status_date_range, report_type
    )


def construct_date_range_status(db_session, start_date, end_date, report_type):
    show_all = False
    if (
        report_type == StatusReport.DATE_SUMMARY_ALL_DATES
        or report_type == StatusReport.DATE_DETAIL_ALL_DATES
        or report_type == StatusReport.DATE_DETAIL_MISSING_PITCHFX
    ):
        show_all = True
    status_date_range = []
    for game_date in get_date_range(start_date, end_date):
        date_status = DateScrapeStatus.find_by_date(db_session, game_date)
        if not date_status:
            error = (
                "scrape_status_date does not contain an entry for date: "
                f"{game_date.strftime(DATE_ONLY)}"
            )
            return Result.Fail(error)
        if not show_all and date_status.scraped_all_game_data:
            continue
        status_date_range.append(date_status)
    return Result.Ok(status_date_range)


def display_date_range_status(db_session, start_date, end_date, status_date_range, report_type):
    if (
        report_type == StatusReport.DATE_DETAIL_MISSING_DATA
        or report_type == StatusReport.DATE_DETAIL_ALL_DATES
    ):
        return display_detailed_report_for_date_range(db_session, status_date_range, False)
    if report_type == StatusReport.DATE_DETAIL_MISSING_PITCHFX:
        return display_detailed_report_for_date_range(db_session, status_date_range, True)
    return display_summary_report_for_date_range(start_date, end_date, status_date_range)


def display_detailed_report_for_date_range(db_session, status_date_range, missing_pitchfx):
    pages = []
    for date_status in status_date_range:
        game_date_str = date_status.game_date.strftime(DATE_MONTH_NAME)
        heading = f"### STATUS REPORT FOR {game_date_str} ###"
        pages.append(DisplayPage(date_status.status_report(), heading))
        missing_ids_str = ""
        if missing_pitchfx:
            heading = f"### %MISSING PITCHFX DATA FOR {game_date_str} ###"
            missing_ids_str = _get_missing_pfx_ids_for_date(db_session, date_status)
            pages.append(DisplayPage(missing_ids_str, heading))
    date_report = PageViewer(
        pages,
        prompt="Press Enter to return to the Main Menu",
        confirm_only=True,
        heading_color="bright_cyan",
        text_color="bright_cyan",
        wrap_text=False,
    )
    if os.environ.get("ENV") == "TEST":
        for page in pages:
            page.display(
                heading_color="bright_cyan",
                text_color="bright_cyan",
                wrap_text=False,
            )
    return Result.Ok(date_report)


def _get_missing_pfx_ids_for_date(db_session, date_status):
    if date_status.scraped_all_pitchfx_logs:
        return ["All PitchFX logs have been scraped"]
    elif date_status.scraped_all_brooks_pitch_logs:
        missing_pitch_app_ids = DateScrapeStatus.get_unscraped_pitch_app_ids_for_date(
            db_session, date_status.game_date
        )
        missing_ids_str = [
            [f"GAME ID: {game_id}", f'{", ".join(pitch_app_id_list)}\n']
            for game_id, pitch_app_id_list in missing_pitch_app_ids.items()
        ]
        return (
            flatten_list2d(missing_ids_str)
            if missing_ids_str
            else ["All PitchFX logs have been scraped"]
        )
    else:
        return ["Missing IDs cannot be reported until all pitch logs have been scraped."]


def display_summary_report_for_date_range(start_date, end_date, status_date_range):
    start_str = start_date.strftime(DATE_MONTH_NAME)
    end_str = end_date.strftime(DATE_MONTH_NAME)
    heading = f"### STATUS REPORT FOR {start_str} - {end_str} ###"
    if not status_date_range:
        all_data_scraped = "All data has been scraped for all dates in the requested range"
        pages = [DisplayPage([all_data_scraped], heading)]
        date_report = PageViewer(
            pages,
            prompt="Press Enter to return to the Main Menu",
            confirm_only=True,
            heading_color="bright_magenta",
            text_color="bright_magenta",
            wrap_text=False,
        )
        return Result.Ok(date_report)
    row_data = [
        {
            "game_date": date_status.game_date_str,
            "status": date_status.scrape_status_description,
        }
        for date_status in status_date_range
    ]
    date_report = DictListTableViewer(
        row_data,
        prompt="Press Enter to return to the Main Menu",
        confirm_only=True,
        heading=heading,
        heading_color="bright_magenta",
        message=None,
        table_color="bright_magenta",
    )
    return Result.Ok(date_report)
