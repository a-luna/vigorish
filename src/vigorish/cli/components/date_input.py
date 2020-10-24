import subprocess
from datetime import datetime

from bullet import colors, Input
from dateutil import parser as date_parser
from getch import pause

from vigorish.cli.components.util import print_heading, print_message

ERROR_HELP_MESSAGE = (
    "You can use any format recognized by dateutil.parser. For example, all of the strings below "
    "are valid ways to represent the same date:\n"
)
EXAMPLES = '"2018-5-13" -or- "05/13/2018" -or- "May 13 2018"\n'


class DateInput(Input):
    """Prompt user for a date value until successfully parsed.

    Date can be provided in any format recognized by dateutil.parser.

    Args:
        prompt (str): Required. Text to display to user before input prompt.
        heading (str): Optional. Bold, underlined text to display above input prompt.
        heading_color (str): Optional. The color of the heading.
        default (datetime): Optional. Default value to use if user provides no input.
        format_str (str): Format string used to display default value, defaults to '%m/%d/%Y'
        indent (int): Distance between left-boundary and start of prompt.
        word_color (str): Optional. The color of the prompt and user input.

    Returns:
        Date: A new Date prompt.
    """

    def __init__(
        self,
        prompt: str,
        heading: str = None,
        heading_color: str = colors.foreground["default"],
        default: datetime = None,
        format_str: str = "%m/%d/%Y",
        indent: int = 0,
        word_color: str = colors.foreground["default"],
    ):
        self.heading = heading
        self.heading_color = heading_color
        if default:
            default = default.strftime(format_str)
        super().__init__(prompt, default=default, indent=indent, word_color=word_color)

    def launch(self):
        while True:
            subprocess.run(["clear"])
            if self.heading:
                print_heading(self.heading, fg=self.heading_color)
            result = super().launch()
            if not result:
                continue
            try:
                date = date_parser.parse(result)
                return date
            except ValueError:
                error = f'\nError: "{result}" could not be parsed as a valid date.'
                print_heading(error, fg="bright_red")
                print_message(ERROR_HELP_MESSAGE, fg="bright_red")
                print_message(EXAMPLES, fg="bright_red")
                pause(message="Press any key to continue...")
