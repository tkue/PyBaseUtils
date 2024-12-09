import os
import csv
import re
import xlrd

import TaskUtil

from enum import Enum

from StringUtil import StringUtil


class EtlTask(TaskUtil.Task):
    pass


class DatabaseDataImport(EtlTask):
    pass


class ImportFileType(Enum):
    CSV = 0
    EXCEL = 1

class DatabaseType(Enum):
    SQLSERVER = 'sqlserver'
    SQLITE = 'sqlite'

class ImportType(Enum):
    EXCEL = 'excel'
    CSV = 'csv'



class ImportFile(object):
    def __int__(self,
                file_path: str,
                file_type: ImportFileType = None):
        self._file_path = file_path
        self._file_type = file_type

    def get_full_path(self):
        return os.path.abspath(self._file_path)


class CsvImportFile(ImportFile):
    def __init__(self,
                 file_path: str):
        super(CsvImportFile, self).__init__(file_path=file_path,
                                            file_type=ImportFileType.CSV)


class SqlServerDataImport(DatabaseDataImport):
    pass


class SqlBuilder(object):
    @staticmethod
    def create_table_from_header(
            table_name,
            database_name,
            header,
            schema_name='',
    ):

        if not table_name.strip():
            return

        sql = []
        insert_name = '{0}.{1}.{2}'.format(database_name, schema_name, table_name)

        sql.append(str('IF OBJECT_ID(\'{0}\') IS NOT NULL\n'
                       '\tDROP TABLE {0};\nGO\n').format(insert_name))

        # Create TableNameID as primary key
        id_name = '{0}ID'.format(table_name)
        # Make sure primary key name is unique
        while id_name in header:
            import uuid
            id_name = '{0}_{1}'.format(id_name, uuid.uuid1().clock_seq_loq)

        sql.append('CREATE TABLE {0} (\n'
                   '\t{1} INT PRIMARY KEY IDENTITY(1, 1)\n'.format(insert_name, id_name))

        for col in header:
            sql.append('\t,[{0}] {1}\n'.format(col, 'NVARCHAR(MAX)'))
        sql.append(')\n')

        sql.append('\nGO\n')

        return ''.join(sql)


    @staticmethod
    def __get_insert_value(val: str, is_quoted: bool, is_null_to_empty_string=False):
        """
        Description:
            Pass in a single value (one column for one row)
            Any chars needing to be escaped are escaped
            Tests the value for encoding errors with StringIO
        :param val: str
            Value you're passing in
        :param is_quoted:
            Whether the value needs to be surrounded by single quotes
            Example:
                INSERT INTO TableName ( [Col1] )
                VALUES ( 'Col1StrVal' )

                "[Col1]" would have is_quoted=False and is also surrounded by (optional) brackets
                "Col1StrVal" would have is_quoted=True, since it is a string and needs quotes

        :param is_null_to_empty_string: bool = True
            If a null (or None) value or an empty string is passed in, this determines if it should be returned as
             either an empty string ('') or as a null (NULL)
        :return:
        """
        val = str(val)
        if not val or not val.strip():
            if is_null_to_empty_string:
                return '\'\''
            return 'NULL'

        val = re.sub("'", "''", val)

        # Don't need brackets for insert statements
        if is_quoted:
            return '\'' + val + '\''

        return str('[' + val + ']')

    @staticmethod
    def get_column_values(header, is_quoted: bool):
        import builtins as b

        if (type(header) == str and not header.strip()) or len(header) == 0:
            return

        if type(header) == b.str:
            header = (header,)

        sql = []
        try:
            for i, col in enumerate(header):
                if i == 0:
                    sql.append(' ' + str(SqlBuilder.__get_insert_value(col, is_quoted)))
                else:
                    sql.append(', ' + str(SqlBuilder.__get_insert_value(col, is_quoted)))
        # TODO: Can probably take out since encoding check is happening in createInsertStatement() \
        #          (may want to change and put in where a single val is escaped)
        except UnicodeDecodeError:
            return SqlBuilder.get_column_values(header, is_quoted)

        return ''.join(sql)

    # TODO: Get rid of appendGo - put this option in when building
    @staticmethod
    def create_insert_statement(tableName, database_name: str, colNames, insertVals, schema_name='', appendGo=True):
        # TODO: Surround datbase_name, schema_name, and tableName with brackets ([])
        # TODO: If schema isNullOrEmpty, don't surround with brackets
        sql = str('INSERT INTO {0}.{1}.{2} (\n{3}\n)\nVALUES (\n{4}\n)\n') \
            .format(database_name,
                    schema_name,
                    tableName,
                    SqlBuilder.get_column_values(colNames, False),
                    SqlBuilder.get_column_values(insertVals, True)
                    )

        if appendGo:
            sql = '{0}GO'.format(sql)

        return sql


class DataImport(object):
    def __init__(self,
                 database_name: str,
                 schema_name: str,
                 table_name: str,
                 column_names: [],
                 rows: [],
                 is_row_contains_column_names: bool = False):
        self.database_name = database_name
        self.schema_name = schema_name
        self.table_name = table_name
        self.column_names = column_names

        if is_row_contains_column_names:
            self.rows = rows[1:]
        else:
            self.rows = rows

    def generate_sql_options(self):
        return 'SET NOCOUNT ON;\n\n'

    def generate_sql_create_table_stmt(self):
        """
        Description:
            Returns string with CREATE TABLE SQL statement
        :return: str
        """
        return SqlBuilder.create_table_from_header(table_name=self.table_name,
                                                   database_name=self.database_name,
                                                   header=self.column_names,
                                                   schema_name=self.schema_name)

    def generate_sql_insert_stmts_list(self):
        """
        Description:
            Returns list of SQL insert statements
        :return: []
        """
        stmts = []
        for r in self.rows:
            stmts.append('{0}{1}'.format(SqlBuilder.create_insert_statement(tableName=self.table_name,
                                                                            database_name=self.database_name,
                                                                            colNames=self.column_names,
                                                                            insertVals=r,
                                                                            schema_name=self.schema_name),
                                         '\n'))
        return stmts

    def generate_all_sql(self):
        """
        Description:
            Return string of CREATE TABLE and INSERT SQL statements
        :return:
        """
        sql = [self.generate_sql_options(), self.generate_sql_create_table_stmt()]
        for r in self.generate_sql_insert_stmts_list():
            sql.append(r)

        return sql

    def write_output_file(self, output_name: str = None):
        if not output_name or not output_name.strip():
            output_name = '{0}.sql'.format(self.table_name)

        with open(output_name, 'w') as f:
            f.write(self.generate_sql_options())
            f.write(self.generate_sql_create_table_stmt())
            for s in self.generate_sql_insert_stmts_list():
                try:
                    f.write(s)
                except Exception as e:
                    print('Failed to write line in file {0}:\n{1}\nException:\n{2}'.format(output_name, s, e))


class CsvImport(object):
    def __init__(self,
                 filename: str,
                 database_name: str,
                 schema_name: str = '',
                 table_name: str = '',
                 ):

        if not table_name or not table_name.strip():
            self.table_name = os.path.basename(filename).split('.')[0]
        else:
            self.table_name = table_name

        self.filename = filename
        self.database_name = database_name

        if not schema_name:
            schema_name = ''
            self.schema_name = schema_name

        if not self.table_name:
            self.table_name = ''

        self.data_import = self.__get_data_import()

    def get_all_sql(self):
        return self.data_import.generate_all_sql()

    def get_sql_insert_stmts_list(self):
        return self.data_import.generate_sql_insert_stmts_list()

    def get_sql_create_table_stmt(self):
        return self.data_import.generate_sql_create_table_stmt()

    def write_output_file(self, output_name: str = None):
        self.data_import.write_output_file(output_name)

    def __get_data_import(self):
        """
        Description:
            Create DataImport class for SQL statements
        :return:
        """
        rows = []
        # TODO: Encoding
        with open(self.filename, 'r', encoding='latin-1') as f:
            csv_file = csv.reader(f)
            try:
                col_names = next(csv_file)
            except UnicodeDecodeError:
                col_names = next([StringUtil.encodingConvertUnicode(str(x)) for x in csv_file])

            for r in csv_file:
                rows.append([str(x) for x in r])

        return DataImport(database_name=self.database_name,
                          schema_name=self.schema_name,
                          table_name=self.table_name,
                          column_names=col_names,
                          rows=rows
                          )

    def __create_sql(self):
        with open(self.filename, 'r') as f:
            csv_file = csv.reader(f)
            self.header = next(csv_file)

            self.sql_create_table = SqlBuilder.create_table_from_header(table_name=self.table_name,
                                                                        database_name=self.database_name,
                                                                        header=self.header,
                                                                        schema_name=self.schema_name)
            for r in csv_file:
                try:
                    self.sql_insert_stmts.append(SqlBuilder.create_insert_statement(tableName=self.table_name,
                                                                                    database_name=self.database_name,
                                                                                    colNames=self.header,
                                                                                    insertVals=r,
                                                                                    schema_name=self.schema_name))
                except Exception as e:
                    print('\nUnable to add insert statement for row:\n{0}'.format(r))
                    print('\nException:\n{0}\n'.format(e))


class ExcelImport(object):
    def __init__(self,
                 filename: str,
                 database_name: str,
                 schema_name: str = '',
                 table_name: str = ''
                 ):
        if not table_name or not table_name.strip():
            self.base_table_name = os.path.basename(filename).split('.')[0]
        else:
            self.base_table_name = table_name

        self.schema_name = schema_name
        self.database_name = database_name

        self.workbook = xlrd.open_workbook(filename)

        self.imports = []

        self.__all_sheets_to_imports()

    def write_output_files(self, path: str):
        if path and os.path.isdir(path):
            os.chdir(path)

        for imp in self.imports:
            imp.write_output_file()

    def get_all_sql(self):
        sql = []
        for imp in self.imports:
            sql.append(imp.generate_all_sql())

        return sql

    def get_table_names(self):
        names = []
        for imp in self.imports:
            names.append(imp.table_name)

        return names

    def __all_sheets_to_imports(self):
        for sheet in self.workbook.sheets():
            self.imports.append(self.__sheet_to_import(sheet))

    def __sheet_to_import(self, worksheet):
        data = [
            [worksheet.cell_value(r, col)
             for col in range(worksheet.ncols)]
            for r in range(worksheet.nrows)
        ]

        table_name = '{0}_{1}'.format(self.base_table_name, worksheet.name)

        return DataImport(database_name=self.database_name,
                          schema_name=self.schema_name,
                          table_name=table_name,
                          column_names=data[0],
                          rows=data[1:]
                          )

    def __get_table_names(self):
        """
        Description:
            Return a list of table names
            This import may have multiple tables since an Excel workbook can have multiple worksheets

        If is_include_sheet_names_in_table_name:
            return BaseTableName_SheetName
        If is_include_sheet_names_in_table_name == False:
            if one sheet:
                return BaseTableName
            else:
                return BaseTableName_N
                    Where N is the index of the sheet
        :return: []
        """
        table_names = []

        for sheet in self.workbook.sheet_names():
            table_names.append('{0}_{1}'.format(self.base_table_name, sheet))

        return table_names

    def write_output_file(self, output_name: str = None):
        self.data_import.write_output_file(output_name)


class ImportFile(object):
    def __init__(self,
                 file_path: str,
                 database_name: str,
                 schema_name: str = '',
                 table_name: str = ''):
        self.file_path = os.path.abspath(file_path)
        self.database_name = database_name
        self.schema_name = schema_name
        self.table_name = table_name

        self.import_type = self.__get_import_type(file_path)
        self.data_import = self.__get_data_import()

    def __get_import_type(filename: str):
        if not filename:
            return

        file_extension = StringUtil.getFileExtension(filename).strip().lower()

        if file_extension in ['xls', 'xlsx']:
            return ImportType.EXCEL
        if file_extension == 'csv':
            return ImportType.CSV

    def __get_data_import(self):
        if self.import_type == ImportType.CSV:
            return CsvImport(self.filename, self.database_name, self.table_name, self.schema_name)

        if self.import_type == ImportType.EXCEL:
            return ExcelImport(self.file_name, database_name=self.database_name, schema_name=self.schema_name,
                               table_name=self.table_name)



