from pathlib import Path

from tabulate import tabulate

from vigorish.cli.components.viewers.display_table import DisplayTable
from vigorish.cli.components.viewers.table_viewer import TableViewer
from vigorish.util.list_helpers import make_chunked_list


class CsvFileTableViewer(TableViewer):
    def __init__(
        self,
        csv_filepath,
        prompt,
        confirm_only=False,
        yes_choice="YES",
        no_choice="NO",
        rows_per_page=10,
        heading=None,
        heading_color=None,
        message_color=None,
        table_color=None,
    ):
        table_list = self.create_tables(csv_filepath, rows_per_page, heading)
        super().__init__(
            table_list=table_list,
            prompt=prompt,
            confirm_only=confirm_only,
            yes_choice=yes_choice,
            no_choice=no_choice,
            heading_color=heading_color,
            message_color=message_color,
            table_color=table_color,
        )

    def create_tables(self, csv_filepath, rows_per_page, heading):
        (col_names, table_rows) = self.get_table_data_from_csv(csv_filepath)
        table_chunks = make_chunked_list(table_rows, chunk_size=rows_per_page)
        return [DisplayTable(tabulate(rows, headers=col_names), heading) for rows in table_chunks]

    def get_table_data_from_csv(self, csv_filepath):
        if isinstance(csv_filepath, str):
            csv_filepath = Path(csv_filepath)
        csv_text = csv_filepath.read_text()
        csv_rows = csv_text.split("\n")
        col_names = [col.strip() for col in csv_rows.pop(0).split(",")]
        table_rows = [row.split(",") for row in csv_rows]
        for num, row in enumerate(table_rows, start=1):
            row.insert(0, num)
        col_names.insert(0, "#")
        return (col_names, table_rows)
