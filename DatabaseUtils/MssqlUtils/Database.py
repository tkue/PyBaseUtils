# import pymssql
import pyodbc
import os
import codecs

from DatabaseUtils.MssqlUtils.Connection import MssqlConnection
from DatabaseUtils.MssqlUtils.mssql_objects.MssqlObjects import Column
from DatabaseUtils.MssqlUtils.SqlObjectType import MssqlScripterObjectType


class Database(object):
    pass

    @staticmethod
    def get_bool_from_string(val: str):
        if not val:
            return

        val = str(val).strip().lower()

        if '1' == val or 'true' == val:
            return True
        else:
            return False


class DatabaseBackupCollection:
    def __init__(self, path: str):
        if not path:
            raise ValueError('Need a value for path')
        if not os.path.exists(path):
            raise ValueError('Path not found: {}'.format(path))

        self._path = path
        self._all_scripts = []
        self.read_files_from_backup_path()

    def get_script(self):
        return '\nGO\n'.join(self._all_scripts)

    def get_files_by_dir_name(self, dir_name: str):
        if not dir_name:
            return

        scripts = []

        for dir in os.listdir(self._path):
            new_path = os.path.abspath(os.path.join(self._path, dir))
            if not os.path.isdir(new_path):
                continue

            if not dir.strip().lower() == dir_name:
                continue

            for file in os.listdir(new_path):
                try:
                    scripts.append(
                        MssqlDatabase.read_script(os.path.abspath(file), is_remove_use_database_statement=True))
                except Exception as e:
                    raise e

        return scripts

    def read_files_from_backup_path(self):
        for file in self.get_files_by_dir_name('database'):
            self._all_scripts.append(file)

        for file in self.get_files_by_dir_name('other'):
            self._all_scripts.append(file)

        for file in self.get_files_by_dir_name('tables'):
            self._all_scripts.append(file)

        for file in self.get_files_by_dir_name('functions'):
            self._all_scripts.append(file)

        for file in self.get_files_by_dir_name('views'):
            self._all_scripts.append(file)

        for file in self.get_files_by_dir_name('procedures'):
            self._all_scripts.append(file)


class MssqlDatabase(Database):

    def __init__(self,
                 connection: MssqlConnection):
        self._connection = connection

    def _get_sql_connection(self):
        return pymssql.connect(self._connection.host,
                               self._connection.username,
                               self._connection.password,
                               self._connection.database)

    def get_database_name(self):
        return self._connection.database

    def get_host_name(self):
        return self._connection.host

    def get_username(self):
        return self._connection.username

    def get_password(self):
        return self._connection.password

    def get_port(self):
        return self._connection.port

    def get_rows_from_sql(self, sql: str, as_dict: bool = False):
        if not sql:
            return

        with self._get_sql_connection() as conn:
            with conn.cursor(as_dict=as_dict) as cur:
                cur.execute(sql)
                row = [row for row in cur]

        return row

    def get_objects(self,
                    from_date: str = None,
                    as_dict: bool = False):
        """

        :param from_date:
        :return:
        """

        from_sql = ''
        # TODO: Don't convert to date and make it so it pulls datetime to compare
        if from_date:
            from_sql = 'WHERE ISNULL(obj.modify_date, obj.create_date) >= TRY_CONVERT(date, \'{}\')'.format(from_date)
        sql = """SELECT 
                    obj.object_id
                    ,obj.type
                    ,FullName = CONCAT(s.Name, '.', obj.name)
                    ,FullNameQuoted = CONCAT(QUOTENAME(s.Name), '.', QUOTENAME(obj.name))
                    ,LastChangeDate = ISNULL(obj.modify_date, obj.create_date)
                FROM sys.objects obj
                JOIN sys.schemas s ON obj.schema_id = s.schema_id
                {}""".format(from_sql)

        return self.get_rows_from_sql(sql, as_dict=as_dict)

    def get_object_metadata(self,
                            object_id: int,
                            as_dict: bool = False):
        if not object_id:
            return

        sql = """SELECT name
                    ,system_type_name
                    ,is_nullable 
                FROM sys.dm_exec_describe_first_result_set_for_object({}, 0) t
                ORDER BY 
                    column_ordinal
        """.format(object_id)

        return self.get_rows_from_sql(sql, as_dict)

    def get_columns_from_object(self, object_id: int):
        metadata = self.get_object_metadata(object_id, as_dict=False)

        if not metadata:
            return

        cols = []

        for row in metadata:
            cols.append(Column(name=row[0],
                               sql=row[1],
                               is_nullable=Database.get_bool_from_string(row[2])))

        return cols

    @staticmethod
    def get_files_from_database_backup_dir(path: str):
        if not path:
            return

        if not os.path.exists(path):
            raise ValueError('path not found: {}'.format(path))

    @staticmethod
    def read_script(path: str,
                    is_remove_use_database_statement: bool = False):
        if not path:
            return

        try:
            lines = []
            with codecs.open(path, 'r', encoding='utf8') as f:
                for line in f.readlines():
                    if is_remove_use_database_statement:
                        if line.strip().lower().startswith('use') or ord(line.strip().lower()[0]) == 65279:
                            continue
                    lines.append(line)
            return lines
        except Exception as e:
            raise type(e)('{}{}'.format(e, ' Unable to read file: {}'.format(path)))

    @staticmethod
    def remove_use_database_from_sql_script(script: str, database: str):
        if not script or not database:
            return

        import re

        regex = re.compile('use\s+\[?{}\]?'.format(database), re.IGNORECASE)
        return regex.sub('', script)