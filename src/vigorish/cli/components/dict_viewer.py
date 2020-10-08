from tabulate import tabulate

from vigorish.cli.components.models import DisplayTable
from vigorish.cli.components.table_viewer import TableViewer
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
        heading=None,
        heading_color=None,
        message=None,
        message_color=None,
        table_color=None,
    ):
        table_list = self.create_tables(dict_list, rows_per_page, heading, message)
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

    def create_tables(self, dict_list, rows_per_page, heading, message):
        (col_names, table_rows) = self.get_table_data_from_dict_list(dict_list)
        table_dicts = [dict(zip(col_names, row)) for row in table_rows if row != [""]]
        table_chunks = make_chunked_list(table_dicts, chunk_size=rows_per_page)
        return [DisplayTable(tabulate(d, headers="keys"), heading, message) for d in table_chunks]

    def get_table_data_from_dict_list(self, dict_list):
        if not dict_list:
            return ([], [])
        col_names = [col for col in dict_list[0].keys()]
        table_rows = [[val for val in row_dict.values()] for row_dict in dict_list]
        for num, row in enumerate(table_rows, start=1):
            row.insert(0, num)
        col_names.insert(0, "#")
        return (col_names, table_rows)
