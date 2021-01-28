from dataclasses import dataclass

from vigorish.cli.components.util import print_heading, print_message


@dataclass(frozen=True)
class DisplayPage:
    text: str
    heading: str = None

    def display(self, heading_color=None, text_color=None, wrap_text=True):
        if self.heading:
            print_heading(self.heading, fg=heading_color)
        for s in self.text:
            print_message(s, fg=text_color, wrap=wrap_text)
