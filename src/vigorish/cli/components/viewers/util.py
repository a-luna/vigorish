from tabulate import tabulate

from vigorish.cli.components.viewers.display_table import DisplayTable
from vigorish.cli.components.viewers.table_viewer import TableViewer


def create_display_table(
    table_rows,
    heading=None,
    message=None,
    table_headers=None,
    tablefmt="fancy_grid",
    numalign="default",
    stralign="default",
    showindex="default",
):
    return DisplayTable(
        table=tabulate(
            table_rows,
            headers=table_headers or (),
            tablefmt=tablefmt,
            numalign=numalign,
            stralign=stralign,
            showindex=showindex,
        ),
        heading=heading,
        message=message,
    )


def create_table_viewer(table_list, table_color="bright_cyan"):
    return TableViewer(
        table_list=table_list,
        prompt="Press Enter to return to previous menu",
        confirm_only=True,
        table_color=table_color,
        heading_color="bright_yellow",
        message_color=None,
    )
