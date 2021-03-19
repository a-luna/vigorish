import vigorish.database as db
from vigorish.cli.components.viewers import DictListTableViewer, DisplayPage, PageViewer
from vigorish.enums import StatusReport
from vigorish.util.datetime_util import get_date_range
from vigorish.util.dt_format_strings import DATE_MONTH_NAME, DATE_ONLY
from vigorish.util.list_helpers import flatten_list2d, make_chunked_list
from vigorish.util.result import Result
from vigorish.util.string_helpers import wrap_text


def report_status_single_date(db_session, game_date, report_type):
    pages = []
    result = _validate_single_date(db_session, game_date)
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
    if report_type in [
        StatusReport.DATE_DETAIL_MISSING_PITCHFX,
        StatusReport.SINGLE_DATE_WITH_GAME_STATUS,
    ]:
        pages.extend(_get_missing_pfx_data_report_for_date(db_session, date_status))
    return Result.Ok(_create_report_viewer(pages, text_color="bright_magenta"))


def report_season_status(db_session, year, report_type):
    if report_type == StatusReport.NONE:
        return Result.Fail("no report")
    season = db.Season.find_by_year(db_session, year)
    if report_type == StatusReport.SEASON_SUMMARY:
        heading = f"### STATUS REPORT FOR {season.name} ###"
        pages = [DisplayPage(season.status_report(), heading)]
        return Result.Ok(_create_report_viewer(pages, text_color="bright_yellow"))
    return report_date_range_status(db_session, season.start_date, season.end_date, report_type)


def report_date_range_status(db_session, start_date, end_date, report_type):
    if report_type == StatusReport.NONE:
        return Result.Fail("no report")
    result = _construct_date_range_status(db_session, start_date, end_date, report_type)
    if result.failure:
        return result
    status_date_range = result.value
    return _get_report_for_date_range(db_session, start_date, end_date, status_date_range, report_type)


def _validate_single_date(db_session, game_date):
    season = db.Season.find_by_year(db_session, game_date.year)
    date_is_valid = db.Season.is_date_in_season(db_session, game_date).success
    date_str = game_date.strftime(DATE_ONLY)
    if not date_is_valid:
        error = (
            f"'{date_str}' is not within the {season.name}:\n"
            f"season_start_date: {season.start_date_str}\n"
            f"season_end_date: {season.end_date_str}"
        )
        return Result.Fail(error)
    date_status = db.DateScrapeStatus.find_by_date(db_session, game_date)
    if not date_status:
        error = f"scrape_status_date does not contain an entry for date: {date_str}"
        return Result.Fail(error)
    return Result.Ok(date_status)


def _construct_date_range_status(db_session, start_date, end_date, report_type):
    show_all = False
    if report_type in [
        StatusReport.DATE_SUMMARY_ALL_DATES,
        StatusReport.DATE_DETAIL_ALL_DATES,
        StatusReport.DATE_DETAIL_MISSING_PITCHFX,
    ]:
        show_all = True
    status_date_range = []
    for game_date in get_date_range(start_date, end_date):
        date_status = db.DateScrapeStatus.find_by_date(db_session, game_date)
        if not date_status:
            error = "scrape_status_date does not contain an entry for date: {game_date.strftime(DATE_ONLY)}"
            return Result.Fail(error)
        if not show_all and date_status.scraped_all_game_data:
            continue
        status_date_range.append(date_status)
    return Result.Ok(status_date_range)


def _get_report_for_date_range(db_session, start_date, end_date, status_date_range, report_type):
    if report_type in [
        StatusReport.DATE_DETAIL_MISSING_DATA,
        StatusReport.DATE_DETAIL_ALL_DATES,
    ]:
        return _get_detailed_report_for_date_range(db_session, status_date_range, False)
    if report_type == StatusReport.DATE_DETAIL_MISSING_PITCHFX:
        return _get_detailed_report_for_date_range(db_session, status_date_range, True)
    return _get_summary_report_for_date_range(start_date, end_date, status_date_range)


def _get_detailed_report_for_date_range(db_session, status_date_range, missing_pitchfx):
    pages = []
    for date_status in status_date_range:
        game_date_str = date_status.game_date.strftime(DATE_MONTH_NAME)
        heading = f"### STATUS REPORT FOR {game_date_str} ###"
        pages.append(DisplayPage(date_status.status_report(), heading))
        if missing_pitchfx:
            pages.extend(_get_missing_pfx_data_report_for_date(db_session, date_status))
    return Result.Ok(_create_report_viewer(pages, text_color="bright_cyan"))


def _get_missing_pfx_data_report_for_date(db_session, date_status):
    game_date_str = date_status.game_date.strftime(DATE_MONTH_NAME)
    heading = f"### MISSING PITCHFX DATA FOR {game_date_str} ###"
    missing_pfx_ids_list = _get_missing_pfx_ids_for_date(db_session, date_status)
    chunked_list = make_chunked_list(missing_pfx_ids_list, chunk_size=4)
    return [DisplayPage(chunk, heading) for chunk in chunked_list]


def _get_missing_pfx_ids_for_date(db_session, date_status):
    if date_status.scraped_all_pitchfx_logs:
        return ["All PitchFX logs have been scraped"]
    if not date_status.scraped_all_brooks_pitch_logs:
        return ["Pitch Appearance IDs without PitchFx data cannot be reported until all pitch logs have been scraped."]
    missing_ids = db.DateScrapeStatus.get_unscraped_pitch_app_ids_for_date(db_session, date_status.game_date)
    return flatten_list2d([_format_id_list(game_id, id_list) for game_id, id_list in missing_ids.items()])


def _format_id_list(game_id, pitch_app_id_list):
    missing_ids = f'MISSING PITCHFX DATA: {", ".join(pitch_app_id_list)}\n'
    return [f"GAME ID: {game_id}", wrap_text(missing_ids, max_len=70)]


def _get_summary_report_for_date_range(start_date, end_date, status_date_range):
    start_str = start_date.strftime(DATE_MONTH_NAME)
    end_str = end_date.strftime(DATE_MONTH_NAME)
    heading = f"### STATUS REPORT FOR {start_str} - {end_str} ###"
    if not status_date_range:
        pages = [DisplayPage(["All data has been scraped for all dates in the requested range"], heading)]
        return Result.Ok(_create_report_viewer(pages, text_color="bright_magenta"))
    dict_list = [{"game_date": ds.game_date_str, "status": ds.scrape_status_description} for ds in status_date_range]
    date_report = DictListTableViewer(
        dict_list,
        prompt="Press Enter to dismiss report",
        confirm_only=True,
        heading=heading,
        heading_color="bright_magenta",
        message=None,
        table_color="bright_magenta",
    )
    return Result.Ok(date_report)


def _create_report_viewer(pages, text_color):
    return PageViewer(
        pages,
        prompt="Press Enter to dismiss report",
        confirm_only=True,
        heading_color=text_color,
        text_color=text_color,
        wrap_text=False,
    )
