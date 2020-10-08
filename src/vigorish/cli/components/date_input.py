import subprocess

from bullet import Input, colors
from dateutil import parser
from getch import pause

from vigorish.cli.components.util import print_heading, print_message
from vigorish.util.dt_format_strings import DATE_ONLY_2


class DateInput(Input):
    """
    Custom Input element that attempts to parse the value provided by the user as a date object.
    Date can be provided in any format recognized by dateutil.parser.
    """

    def __init__(
        self,
        prompt,
        heading=None,
        heading_color=None,
        default=None,
        indent=0,
        word_color=colors.foreground["default"],
        strip=False,
        pattern="",
    ):
        self.heading = heading
        self.heading_color = heading_color
        if default:
            default = default.strftime(DATE_ONLY_2)
        super().__init__(prompt, default, indent, word_color, strip, pattern)

    def launch(self):
        while True:
            subprocess.run(["clear"])
            if self.heading:
                print_heading(self.heading, fg=self.heading_color)
            result = super().launch()
            if not result:
                continue
            try:
                parsed_date = parser.parse(result)
                return parsed_date
            except ValueError:
                error = f'\nError: "{result}" could not be parsed as a valid date.'
                error_help = (
                    "You can use any format recognized by dateutil.parser. For example, all of "
                    "the strings below are valid ways to represent the same date:\n"
                )
                examples = '"2018-5-13" -or- "05/13/2018" -or- "May 13 2018"\n'
                print_heading(error, fg="bright_red")
                print_message(error_help, fg="bright_red")
                print_message(examples, fg="bright_red")
                pause(message="Press any key to continue...")
