from bullet import Input, Check, colors
from dateutil import parser
from getch import pause

from vigorish.cli.util import print_message
from vigorish.util.dt_format_strings import DATE_ONLY_2
from vigorish.util.regex import JOB_NAME_REGEX

DEFAULT_JOB_NAME = "Optional - Press enter to use default name"


class DateInput(Input):
    def __init__(
        self,
        prompt,
        default=None,
        indent=0,
        word_color=colors.foreground["default"],
        strip=False,
        pattern="",
    ):
        if default:
            default = default.strftime(DATE_ONLY_2)
        super().__init__(prompt, default, indent, word_color, strip, pattern)

    def launch(self):
        while True:
            result = super().launch()
            if not result:
                continue
            try:
                parsed_date = parser.parse(result)
                return parsed_date
            except ValueError:
                error = (
                    f'"{result}" could not be parsed as a valid date. You can use any format '
                    f"recognized by dateutil.parser. For example, all of the strings below "
                    "are valid ways to represent the same date:\n"
                )
                examples = '"2018-5-13" -or- "05/13/2018" -or- "May 13 2018"'
                print_message(error, fg="bright_red")
                print_message(examples, fg="bright_red")
                pause(message="Press any key to continue...")


class JobNameInput(Input):
    def __init__(
        self,
        prompt,
        default=None,
        indent=0,
        word_color=colors.foreground["default"],
        strip=False,
        pattern="",
    ):
        if not default:
            default = DEFAULT_JOB_NAME
        super().__init__(prompt, default, indent, word_color, strip, pattern)

    def launch(self):
        while True:
            result = super().launch()
            if result == self.default:
                return result
            if JOB_NAME_REGEX.match(result):
                return result
            error = (
                f"'{result}' contains one or more invalid characters. Job name must contain "
                "only letters, numbers, hyphen and underscore characters.\n"
            )
            print_message(error, fg="bright_red")
            pause(message="Press any key to continue...")


class DataSetCheck(Check):
    def __init__(
        self,
        prompt: str = "",
        choices: list = [],
        checked_data_sets: dict = {},
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
