from tabulate import tabulate

from vigorish.cli.components.viewers.display_table import DisplayTable
from vigorish.cli.components.viewers.table_viewer import TableViewer
from vigorish.util.list_helpers import make_chunked_list


class DictListTableViewer(TableViewer):
    def __init__(
        self,
        dict_list,
        prompt=None,
        confirm_only=False,
        yes_choice="YES",
        no_choice="NO",
        rows_per_page=10,
        row_numbers=False,
        heading=None,
        heading_color=None,
        message=None,
        message_color=None,
        table_color=None,
    ):
        super().__init__(
            table_list=self.create_table_list(dict_list, rows_per_page, row_numbers, heading, message),
            prompt=prompt,
            confirm_only=confirm_only,
            yes_choice=yes_choice,
            no_choice=no_choice,
            heading_color=heading_color,
            message_color=message_color,
            table_color=table_color,
        )

    def create_table_list(self, dict_list, rows_per_page, row_numbers, heading, message):
        table_dicts = self.create_table_dicts(dict_list, row_numbers)
        table_chunks = make_chunked_list(table_dicts, chunk_size=rows_per_page)
        return [DisplayTable(tabulate(d, headers="keys"), heading, message) for d in table_chunks]

    def create_table_dicts(self, dict_list, row_numbers):
        if not dict_list:
            return ([], [])
        if not row_numbers:
            return dict_list
        col_names = list(dict_list[0].keys())
        table_rows = [list(row_dict.values()) for row_dict in dict_list]
        for num, row in enumerate(table_rows, start=1):
            row.insert(0, num)
        col_names.insert(0, "#")
        return [dict(zip(col_names, row)) for row in table_rows if row != [""]]
