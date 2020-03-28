from vigorish.cli.util import print_message
from vigorish.config.database import Season, DateScrapeStatus
from vigorish.enums import StatusReport
from vigorish.status.update_status import update_status_for_mlb_season
from vigorish.util.datetime_util import get_date_range
from vigorish.util.dt_format_strings import DATE_MONTH_NAME
from vigorish.util.result import Result


def report_status_single_date(
    session, scraped_data, refresh_data, game_date, missing_pitchfx, with_games
):
    season = Season.find_by_year(session, game_date.year)
    date_is_valid = Season.is_date_in_season(session, game_date).success
    date_str = game_date.strftime(DATE_ONLY)
    if not date_is_valid:
        error = (
            f"'{date_str}' is not within the {season.name}:\n"
            f"season_start_date: {season.start_date_str}\n"
            f"season_end_date: {season.end_date_str}"
        )
        return Result.Fail(error)
    if refresh_data:
        result = refresh_season_data(session, scraped_data, season.year)
        if result.failure:
            return result
    date_status = DateScrapeStatus.find_by_date(session, game_date)
    if not date_status:
        error = f"scrape_status_date does not contain an entry for date: {date_str}"
        return Result.Fail(error)
    if missing_pitchfx:
        if date_status.scraped_all_pitchfx_logs:
            missing_ids_str = "All PitchFX logs have been scraped"
        elif date_status.scraped_all_brooks_pitch_logs:
            missing_pitch_app_ids = DateScrapeStatus.get_unscraped_pitch_app_ids_for_date(
                session, game_date
            )
            missing_ids_str = (
                f"MISSING: {missing_pitch_app_ids}"
                if missing_pitch_app_ids
                else "All PitchFX logs have been scraped"
            )
        else:
            missing_ids_str = (
                "Missing IDs cannot be reported until all pitch logs have been scraped."
            )
    date_str = game_date.strftime(DATE_MONTH_NAME)
    print_message(f"\n### STATUS REPORT FOR {date_str} ###", fg="bright_cyan", bold=True)
    print_message(date_status.status_report(), fg="bright_cyan")
    if missing_ids:
        print_message(missing_ids_str, fg="bright_cyan")
    if with_games:
        print_message(date_status.games_status_report(), fg="bright_cyan")
    return Result.Ok()


def report_season_status(session, scraped_data, refresh_data, year, report_type):
    if report_type == StatusReport.NONE:
        return Result.Ok()
    season = Season.find_by_year(session, year)
    if report_type == StatusReport.SEASON_SUMMARY:
        if refresh_data:
            result = refresh_season_data(session, scraped_data, season.year)
            if result.failure:
                return result
        print_message(f"\n### STATUS REPORT FOR {season.name} ###", fg="bright_yellow", bold=True)
        print_message(season.status_report(), fg="bright_yellow")
        return Result.Ok()
    start_date = season.start_date
    end_date = season.end_date
    return report_date_range_status(
        session, scraped_data, refresh_data, start_date, end_date, report_type
    )


def report_date_range_status(
    session, scraped_data, refresh_data, start_date, end_date, report_type
):
    if report_type == StatusReport.NONE:
        error = f"Invalid value for verbosity: {report_type}. Value must be greater than zero."
        return Result.Fail(error)
    result = construct_date_range_status(
        session, scraped_data, refresh_data, start_date, end_date, report_type
    )
    if result.failure:
        return result
    status_date_range = result.value
    return display_date_range_status(session, start_date, end_date, status_date_range, report_type)


def construct_date_range_status(
    session, scraped_data, refresh_data, start_date, end_date, report_type
):
    result = Season.validate_date_range(session, start_date, end_date)
    if result.failure:
        return result
    season = result.value
    if refresh_data:
        result = refresh_season_data(session, scraped_data, season.year)
        if result.failure:
            return result
    show_all = False
    if (
        report_type == StatusReport.DATE_SUMMARY_ALL_DATES
        or report_type == StatusReport.DATE_DETAIL_ALL_DATES
        or report_type == StatusReport.DATE_DETAIL_MISSING_PITCHFX
    ):
        show_all = True
    status_date_range = []
    for game_date in get_date_range(start_date, end_date):
        date_status = DateScrapeStatus.find_by_date(session, game_date)
        if not date_status:
            error = f"scrape_status_date does not contain an entry for date: {game_date.strftime(DATE_ONLY)}"
            return Result.Fail(error)
        if not show_all and date_status.scraped_all_game_data:
            continue
        status_date_range.append(date_status)
    return Result.Ok(status_date_range)


def display_date_range_status(session, start_date, end_date, status_date_range, report_type):
    if (
        report_type == StatusReport.DATE_DETAIL_MISSING_DATA
        or report_type == StatusReport.DATE_DETAIL_ALL_DATES
    ):
        return display_detailed_report_for_date_range(session, status_date_range, False)
    if report_type == StatusReport.DATE_DETAIL_MISSING_PITCHFX:
        return display_detailed_report_for_date_range(session, status_date_range, True)
    return display_summary_report_for_date_range(session, start_date, end_date, status_date_range)


def display_detailed_report_for_date_range(session, status_date_range, missing_pitchfx):
    for date_status in status_date_range:
        game_date_str = date_status.game_date.strftime(DATE_MONTH_NAME)
        if missing_pitchfx:
            if date_status.scraped_all_pitchfx_logs:
                missing_ids_str = "All PitchFX logs have been scraped"
            elif date_status.scraped_all_brooks_pitch_logs:
                missing_pitch_app_ids = DateScrapeStatus.get_unscraped_pitch_app_ids_for_date(
                    session, game_date
                )
                missing_ids_str = (
                    f"MISSING: {missing_pitch_app_ids}"
                    if missing_pitch_app_ids
                    else "All PitchFX logs have been scraped"
                )
            else:
                missing_ids_str = (
                    "Missing IDs cannot be reported until all pitch logs have been scraped."
                )
        print_message(f"\n### STATUS REPORT FOR {game_date_str} ###", fg="bright_cyan", bold=True)
        print_message(date_status.status_report(), fg="bright_cyan")
        if missing_pitchfx:
            print_message(missing_ids_str, fg="bright_cyan")
    return Result.Ok()


def display_summary_report_for_date_range(session, start_date, end_date, status_date_range):
    start_str = start_date.strftime(DATE_MONTH_NAME)
    end_str = end_date.strftime(DATE_MONTH_NAME)
    print_message(
        f"\n### STATUS REPORT FOR {start_str} - {end_str} ###", fg="bright_magenta", bold=True
    )
    if not status_date_range:
        print_message("All data has been scraped for all dates in the requested range")
        return Result.Ok()
    for status in status_date_range:
        date_str = status.game_date_str
        status_description = status.scrape_status_description
        print_message(f"{date_str}: {status_description}", fg="bright_magenta")
    return Result.Ok()


def refresh_season_data(session, scraped_data, year):
    result = update_status_for_mlb_season(session, scraped_data, year)
    if result.failure:
        return result
    return Result.Ok()