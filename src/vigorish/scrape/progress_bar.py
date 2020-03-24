from vigorish.enums import DataSet

PBAR_LEN_DICT = {
    DataSet.BBREF_GAMES_FOR_DATE: 34,
    DataSet.BBREF_BOXSCORES: 29,
    DataSet.BROOKS_GAMES_FOR_DATE: 35,
    DataSet.BROOKS_PITCH_LOGS: 31,
    DataSet.BROOKS_PITCHFX: 28,
}


def game_date_description(date, data_set):
    pre = f"Game Date | {date.strftime(MONTH_NAME_SHORT)}"
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


def url_fetch_description(data_set, display_id):
    pre = f"FETCHING  | {display_id}"
    pad_len = PBAR_LEN_DICT[data_set] - len(pre)
    return f"{pre}{'.'*pad_len}"


def url_parse_description(data_set, display_id):
    pre = f"Parsing   | {display_id}"
    pad_len = PBAR_LEN_DICT[data_set] - len(pre)
    return f"{pre}{'.'*pad_len}"


def updating_description(data_set, display_id):
    pre = f"Updating  | {display_id}"
    pad_len = PBAR_LEN_DICT[data_set] - len(pre)
    return f"{pre}{'.'*pad_len}"


def save_html_description(data_set, display_id):
    pre = f"Save HTML | {display_id}"
    pad_len = PBAR_LEN_DICT[data_set] - len(pre)
    return f"{pre}{'.'*pad_len}"


def save_json_description(data_set, display_id):
    pre = f"Save JSON | {display_id}"
    pad_len = PBAR_LEN_DICT[data_set] - len(pre)
    return f"{pre}{'.'*pad_len}"
