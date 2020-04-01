from vigorish.enums import DataSet
from vigorish.util.dt_format_strings import DATE_MONTH_NAME

PBAR_LEN_DICT = {
    DataSet.BBREF_GAMES_FOR_DATE: 34,
    DataSet.BBREF_BOXSCORES: 29,
    DataSet.BROOKS_GAMES_FOR_DATE: 35,
    DataSet.BROOKS_PITCH_LOGS: 31,
    DataSet.BROOKS_PITCHFX: 28,
}


def game_date_description(date, data_set):
    pre = f"Game Date | {date.strftime(DATE_MONTH_NAME)}"
    pad_len = PBAR_LEN_DICT[data_set] - len(pre)
    return f"{pre}{'.'*pad_len}"


def data_set_description(data_set):
    pre = f"Data Set  | {data_set}"
    pad_len = PBAR_LEN_DICT[data_set] - len(pre)
    return f"{pre}{'.'*pad_len}"


def url_local_description(data_set, display_id):
    pre = f"FROM FILE | {display_id}"
    pad_len = PBAR_LEN_DICT[data_set] - len(pre)
    return f"{pre}{'.'*pad_len}"


def url_s3_description(data_set, display_id):
    pre = f"FROM S3   | {display_id}"
    pad_len = PBAR_LEN_DICT[data_set] - len(pre)
    return f"{pre}{'.'*pad_len}"


def url_batch_description(data_set, batch_size, is_current_batch):
    size_str = f"{batch_size}" if batch_size > 9 else f" {batch_size}"
    batch_context = "this" if is_current_batch else "next"
    pre = f"{size_str} URLs {batch_context} batch"
    pad_len = PBAR_LEN_DICT[data_set] - len(pre)
    return f"{pre}{' '*(pad_len+4)}"


def batch_timeout_description(data_set, description):
    pad_len = PBAR_LEN_DICT[data_set] - len(description)
    return f"{description}{' '*(pad_len+4)}"


def url_fetch_description(data_set, display_id):
    pre = f"{display_id} (Fetch HTML) "
    pad_len = PBAR_LEN_DICT[data_set] - len(pre)
    return f"{pre}"


def url_parse_description(data_set, display_id):
    pre = f"Parsing   | {display_id}"
    pad_len = PBAR_LEN_DICT[data_set] - len(pre)
    return f"{pre}{'.'*pad_len}"


def updating_description(data_set, display_id):
    pre = f"Updating  | {display_id}"
    pad_len = PBAR_LEN_DICT[data_set] - len(pre)
    return f"{pre}{'.'*pad_len}"


def save_html_description(data_set, display_id):
    pre = f"{display_id} (Save HTML) "
    pad_len = PBAR_LEN_DICT[data_set] - len(pre)
    return f"{pre}"


def save_json_description(data_set, display_id):
    pre = f"Save JSON | {display_id}"
    pad_len = PBAR_LEN_DICT[data_set] - len(pre)
    return f"{pre}{'.'*pad_len}"
