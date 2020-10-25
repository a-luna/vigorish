import subprocess

from bullet.cursor import hide as cursor_hidden
from bullet.utils import moveCursorUp

from vigorish.cli.components.page_viewer import PageViewer


class TableViewer(PageViewer):
    def __init__(
        self,
        table_list,
        prompt,
        confirm_only=False,
        yes_choice="YES",
        no_choice="NO",
        heading_color=None,
        message_color=None,
        table_color=None,
    ):
        super(TableViewer, self).__init__(
            pages=table_list,
            prompt=prompt,
            confirm_only=confirm_only,
            yes_choice=yes_choice,
            no_choice=no_choice,
            heading_color=heading_color,
            text_color=message_color,
        )
        self.table_color = table_color

    def launch(self):
        while True:
            subprocess.run(["clear"])
            self.current_page.display(
                heading_color=self.heading_color,
                message_color=self.text_color,
                table_color=self.table_color,
            )
            self.print_page_nav()
            self.needs_update = False
            self.print_prompt()
            if not self.confirm_only:
                self.renderBullets()
            moveCursorUp(len(self.choices) - self.pos)
            with cursor_hidden():
                while True:
                    user_selection = self.handle_input()
                    if self.needs_update:
                        break
                    if user_selection is not None:
                        return self.choices_dict.get(user_selection)
