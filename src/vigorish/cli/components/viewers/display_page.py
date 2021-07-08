from dataclasses import dataclass

from vigorish.cli.components.util import print_heading, print_message


@dataclass(frozen=True)
class DisplayPage:
    text: str
    heading: str = None
    wrap: bool = True

    def display(self, heading_color=None, text_color=None):
        if self.heading:
            print_heading(self.heading, fg=heading_color)
        for s in self.text:
            print_message(s, fg=text_color, wrap=self.wrap)
