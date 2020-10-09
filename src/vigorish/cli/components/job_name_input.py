"""CLI elements that extend Bullet classes for"""
import subprocess

from bullet import colors, Input
from getch import pause

from vigorish.cli.components.util import print_error, print_heading, print_message
from vigorish.util.regex import JOB_NAME_REGEX

DEFAULT_JOB_NAME = "use default"


class JobNameInput(Input):
    """
    Custom Input element that accepts strings consisting of ONLY a-z. A-Z, 0-9, hyphen ("-")
    and/or underscore ("_") characters. Also, user is allowed to provide no value and dismiss
    the prompt, since the JobName value the name refers to is an optional value on the
    SQLAlchemy model for the ScrapeJob class.
    """

    def __init__(
        self,
        prompt,
        heading=None,
        heading_color=None,
        message=None,
        default=None,
        indent=0,
        word_color=colors.foreground["default"],
        strip=False,
        pattern="",
    ):
        self.heading = heading
        self.heading_color = heading_color
        self.message = message
        if not default:
            default = DEFAULT_JOB_NAME
        super().__init__(prompt, default, indent, word_color, strip, pattern)

    def launch(self):
        while True:
            subprocess.run(["clear"])
            if self.heading:
                print_heading(self.heading, fg=self.heading_color)
            if self.message:
                print_message(f"{self.message}\n")
            result = super().launch()
            if result == DEFAULT_JOB_NAME:
                return None
            if JOB_NAME_REGEX.match(result):
                return result
            error = (
                f"\n'{result}' contains one or more invalid characters. Job name must contain "
                "only letters, numbers, hyphen and underscore characters.\n"
            )
            print_error(error, fg="bright_red")
            pause(message="Press any key to continue...")
