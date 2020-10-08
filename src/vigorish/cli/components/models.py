from dataclasses import dataclass

from vigorish.cli.components.util import print_heading, print_message


@dataclass(frozen=True)
class DisplayPage:
    text: str
    heading: str = None

    def display(self, heading_color=None, text_color=None):
        if self.heading:
            print_heading(self.heading, fg=heading_color)
        for s in self.text:
            print_message(s, fg=text_color)


@dataclass(frozen=True)
class DisplayTable:
    table: str
    heading: str = None
    message: str = None

    def display(self, heading_color=None, message_color=None, table_color=None):
        if self.heading:
            print_heading(self.heading, fg=heading_color)
        if self.message:
            print_message(self.message, fg=message_color, wrap=False)
            print()
        print_message(self.table, fg=table_color, wrap=False)
