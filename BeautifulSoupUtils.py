from enum import Enum


class ParseTableType(Enum):
    HEADERS_COLUMNS = 'headers_columns'
    NO_HEADER = 'no_header'
    HEADER_ONLY = 'header_only'


class TableUtil(object):
    @staticmethod
    def __parse_table_full(table, parse_table_type: ParseTableType):
        if not table or not parse_table_type:
            return
        types_tofind = None
        if parse_table_type == ParseTableType.HEADERS_COLUMNS:
            types_tofind = ('th', 'td')
        elif parse_table_type == ParseTableType.NO_HEADER:
            types_tofind = 'td'
        elif parse_table_type == ParseTableType.HEADER_ONLY:
            types_tofind = 'th'

        return [
            [cell.get_text().strip() for cell in row.find_all(types_tofind) if cell]
            for row in table.find_all('tr')
            ]

    @staticmethod
    def get_table_alldata(table):
        return TableUtil.__parse_table_full(table, ParseTableType.HEADERS_COLUMNS)

    @staticmethod
    def get_table_noheader(table):
        return TableUtil.__parse_table_full(table, ParseTableType.NO_HEADER)

    # FIXME: Stop from inserting blank rows (inserts header then all other rows as empty rows)
    @staticmethod
    def get_table_headeronly(table):
        return TableUtil.__parse_table_full(table, ParseTableType.HEADER_ONLY)