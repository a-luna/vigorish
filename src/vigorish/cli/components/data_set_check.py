from bullet import Check, colors


class DataSetCheck(Check):
    """Custom Check element that allows a user to select one or more DataSet values."""

    def __init__(
        self,
        prompt: str = "",
        choices: list = None,
        checked_data_sets: dict = None,
        check: str = "√",
        check_color: str = colors.foreground["default"],
        check_on_switch: str = colors.REVERSE,
        word_color: str = colors.foreground["default"],
        word_on_switch: str = colors.REVERSE,
        background_color: str = colors.background["default"],
        background_on_switch: str = colors.REVERSE,
        pad_right=0,
        indent: int = 0,
        align=0,
        margin: int = 0,
        shift: int = 0,
    ):
        super().__init__(
            prompt,
            choices,
            check,
            check_color,
            check_on_switch,
            word_color,
            word_on_switch,
            background_color,
            background_on_switch,
            pad_right,
            indent,
            align,
            margin,
            shift,
        )
        if checked_data_sets:
            self.checked = [choice in checked_data_sets for choice in choices]
