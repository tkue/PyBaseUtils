import logging
import subprocess
from enum import Enum

import pyodbc

from DatabaseUtils.DatabaseType import DatabaseType

import codecs
import os

import json

class MssqlScripterArguments(Enum):
    SERVER = '--server'
    DATABASE = '--database'
    USER = '--user'
    PASSWORD = '--password'
    FILE_PATH = '--file-path'
    FILE_PER_OBJECT = '--file-per-object'
    SCRIPT_CREATE = '--script-create'
    SCRIPT_DROP = '--script-drop'
    SCRIPT_DROP_CREATE = '--script-drop-create'
    INCLUDE_OBJECTS = '--include-objects'
    EXCLUDE_OBJECTS = '--exclude-objects'
    INCLUDE_SCHEMAS = '--include-schemas'
    EXCLUDE_SCHEMAS = '--exclude-schemas'
    INCLUDE_TYPES = '--include-types'
    EXCLUDE_TYPES = '--exclude-types'
    ANSI_PADDING = '--ansi-padding'
    APPEND = '--append'
    CHECK_FOR_EXISTENCE = '--check-for-existence'
    CONTINUE_ON_ERROR = '--continue-on-error'
    EXCLUDE_HEADERS = '--exclude-headers'
    OBJECT_PERMISSIONS = '--object-permissions'
    OWNER = '--owner'
    EXCLUDE_USE_DATABASE = '--exclude-use-database'
    CHANGE_TRACKING = '--change-tracking'
    DATA_COMPRESSIONS = '--data-compressions'
    DISPLAY_PROGRESS = '--display-progress'
    DATA_ONLY = '--data-only'
    SCHEMA_AND_DATA = '--schema-and-data'

# TODO: Is this all the defined types?
class MssqlScripterObjectType(Enum):
    USER_DEFINED_FUNCTION = 'UserDefinedFunction'
    STORED_PROCEDURE = 'StoredProcedure'
    VIEW = 'View'
    TABLE = 'Table'
    DATABASE = 'Database'
    SCHEMA = 'Schema'
    EXTENDED_PROPERTY = 'ExtendedProperty'
    FULL_TEXT_CATALOG = 'FullTextCatalog'
    XML_SCHEMA_COLLECTION = 'XmlSchemaCollection'
    USER_DEFINED_DATA_TYPE = 'UserDefinedDataType'
    DDL_TRIGGER = 'UserDefinedDataType'



class Database(object):
    _config_json: dict
    _base_path = 'deploy'
    _schema_dir = os.path.join(_base_path, '00000_schema')
    _objects_dir = os.path.join(_base_path, '00000_objects')


    def __init__(self,
                 database_type: DatabaseType,
                 server: str,
                 database: str,
                 username: str,
                 password: str,
                 port: int,
                 local_path: str,
                 logger: logging.Logger = None,
                 ):
        self._database_type = database_type

        if logger:
            self.logger = logger
        else:
            import logging
            logger = logging.getLogger(__name__)

            self.logger = logger

        self._server = server
        self._database = database
        self._username = username
        self._password = password
        self._port = port
        self._database_type = database_type
        self._local_path = local_path


    @property
    def server(self):
        return self._server

    @property
    def port(self):
        return self._port

    @property
    def database_type(self):
        return self._database_type

    @property
    def local_path(self):
        return self._local_path

    @property
    def database(self):
        return self._database

    @property
    def username(self):
        return self._username

    @property
    def password(self):
        return self._password

    @property
    def database_type(self):
        return self._database_type

    @database_type.setter
    def database_type(self, val: DatabaseType):
        if not val:
            return

        if not DatabaseType.MSSQL.value == 'mssql':
            raise ValueError('Expecting database type to DatabaseType.MSSQL, but got {}'.format(val.value))

        self._database_type = val

    def _get_driver(self):
        if self.database_type.value == DatabaseType.MSSQL.value:
            return '{ODBC Driver 17 for SQL Server}'
            # return '{SQL Server}'
        else:
            raise NotImplementedError

    def _get_connection_string(self):
        if self.database_type.value == DatabaseType.MSSQL.value:
            return 'DRIVER=' + self._get_driver() + ';SERVER=' + self.server + ';DATABASE=' + self.database + ';UID=' + self._username + ';PWD=' + self._password

        raise NotImplementedError

    def _get_conn(self):
        return pyodbc.connect(self._get_connection_string())

    def get_rows_from_sql(self, sql: str):
        if not sql:
            return

        with self._get_conn() as conn:
            with conn.cursor() as cur:
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
        from DatabaseUtils.MssqlUtils.mssql_objects.MssqlObjects import Column

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
    def get_sql_file_paths_from_dir(path: str):
        if not path:
            return

        import os

        files = []
        for file in os.listdir(path):
            files.append(os.path.abspath(os.path.join(path, file)))

        return sorted(files)

    @staticmethod
    def get_bool_from_string(val: str):
        if not val:
            return

        val = str(val).strip().lower()

        if '1' == val or 'true' == val:
            return True
        else:
            return False
    @staticmethod
    def get_sql_from_file(path: str):
        if not path:
            return

        sql = []
        with open(path, 'r') as f:
            is_start_to_read_file = False
            for line in f.readlines():
                if str(line).strip().lower().startswith('go'):
                    continue

                if is_start_to_read_file:
                    sql.append(line)
                    continue
                else:
                    if str(line).strip().lower().startswith('create'):
                        is_start_to_read_file = True
                        sql.append(line)
                    else:
                        continue

        return ''.join(sql)

    @staticmethod
    def get_all_sql_from_files(path: str):
        if not path:
            return

        sql = []

        for file in Database.get_sql_file_paths_from_dir(path):
            sql.append(Database.get_sql_from_file(os.path.abspath(file)))

        return sql.join('\nGO\n')

    def get_schema_changes_file_paths(self):
        return Database.get_sql_file_paths_from_dir(os.path.join(self.local_path, self._schema_dir))

    def get_object_changes_file_paths(self):
        return Database.get_sql_file_paths_from_dir(os.path.join(self.local_path, self._objects_dir))

    def get_all_file_paths(self):
        paths = []
        for path in self.get_schema_changes_file_paths():
            paths.append(path)
        for path in self.get_object_changes_file_paths():
            paths.append(path)

        return paths

    def get_all_sql_as_list(self):
        sql = []

        for file in self.get_schema_changes_file_paths():
            sql.append(Database.get_sql_from_file(file))

        for file in self.get_object_changes_file_paths():
            sql.append(Database.get_sql_from_file(file))

        return sql

    def get_all_sql_as_string(self):
        return ';\n'.join(self.get_all_sql_as_list())

    def get_sql_file_basenames(self):
        for file in self.get_all_file_paths():
            return os.path.basename(file).replace('.sql', '')

    def execute_scalar(self, sql: str):
        cur = self._get_conn()
        try:
            cur.execute(sql)
            cur.commit()
        except Exception as e:
            cur.rollback()
            raise e
        finally:
            cur.close()

    def deploy_to_database(self):
        for sql in self.get_all_sql_as_list():
            self.execute_scalar(sql)

    @staticmethod
    def generate_in_string_from_list(values):
        """
        If pass in:
            values = [10, 20, 30]

        returns string like:
            IN ('10', '20', '30')
        """
        if not values:
            return

        print('Values: {}'.format(values))

        values = list(values)

        print(values)

        to_delete_string = None

        for val in values:
            id_str = "'{}'".format(val)
            if not to_delete_string:
                to_delete_string = id_str
            else:
                to_delete_string += ',{}'.format(id_str)

        return '({})'.format(to_delete_string)

    @staticmethod
    def get_files_from_database_backup_dir(path: str):
        if not path:
            return

        if not os.path.exists(path):
            raise ValueError('path not found: {}'.format(path))

    @staticmethod
    def get_databases_from_json_config(file_path: str, name: str = None):
        """
        Parses JSON config settings and returns array of object Database
        :param file_path:
        :param name:
        :return:
        """
        if not file_path:
            return

        import json

        databases = []

        if not os.path.exists(file_path):
            raise os.error('file_path does not exists: {}'.format(file_path))

        with open(file_path, 'r') as f:
            cfg = f.readlines()

        j = json.loads(''.join(cfg))

        if name:
            name = name.strip().lower()

        import logging
        logger = logging.getLogger(__name__)


        for db in j['databases']:
            if name:
                if str(j['alias']).strip().lower() != name or str(j['name']).strip().lower() != name:
                    continue

            databaseType = Database.get_database_type_by_string_value(db['database_type'])
            if not databaseType:
                databaseType = DatabaseType.GENERIC

            if databaseType.value == DatabaseType.MSSQL.value:
                dbObj = MssqlDatabase(server=db['server'],
                                                database=db['name'],
                                                username=db['username'],
                                                password=db['password'],
                                                port=db['port'],
                                                local_path=db['local_path'])
            else:
                dbObj = Database(DatabaseType.GENERIC.value,
                                                    logger=logger,
                                                    server=db['server'],
                                                    database=db['name'],
                                                    username=db['username'],
                                                    password=db['password'],
                                                    port=db['port'],
                                                    local_path=db['local_path'])
            databases.append(dbObj)


        return databases

    @staticmethod
    def get_database_type_by_string_value(database_type: str):
        if not database_type:
            return

        database_type = database_type.strip().lower()

        for val in DatabaseType:
            if str(val.value).strip().lower() == database_type:
                return val

    @staticmethod
    def get_database_type_from_string(val: str):
        if not val:
            return

        for item in DatabaseType:
            if val.strip().lower() == item.value.strip().lower():
                return item



class MssqlDatabase(Database):
    _pyodbc_driver = '{ODBC Driver 17 for SQL Server}'
    __default_mssqlscripter_options = [
        MssqlScripterArguments.EXCLUDE_HEADERS.value
    ]
    # TODO: Map to multiple types
    __sub_folders = {
        'database': MssqlScripterObjectType.DATABASE,
        'tables': MssqlScripterObjectType.TABLE,
        'views': MssqlScripterObjectType.VIEW,
        'functions': MssqlScripterObjectType.USER_DEFINED_FUNCTION,
        'procedures': MssqlScripterObjectType.STORED_PROCEDURE
    }

    def __init__(self,
                 server: str,
                 database: str,
                 username: str,
                 password: str,
                 port: int=None,
                 local_path: str=None
                 ):
        if not port or port == 0:
            port = 1433

        super(MssqlDatabase, self).__init__(server=server,
                                            database=database,
                                            username=username,
                                            password=password,
                                            port=port,
                                            database_type=DatabaseType.MSSQL,
                                            local_path=local_path)


    def _get_connection_string(self):
        return 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self.server + ';DATABASE=' + self.database + ';UID=' + self._username + ';PWD=' + self._password

    def __get_sqlcmd_start_args(self):

        args = []
        args.append('sqlcmd')
        args.append('-S')
        args.append(self.server)
        args.append('-d')
        args.append(self.database)
        args.append('-U')
        args.append(self.username)
        args.append('-P')
        args.append(self.password)

        return args

    def execute_script(self, path: str):
        if not path:
            return

        # args = self.__get_sqlcmd_start_args()
        # args += ' -i ' + path
        args = self.__get_sqlcmd_start_args()

        args.append('-i')
        args.append(path)
        self.logger.info('Executing using sqlcmd: {}'.format(' '.join(args)))
        subprocess.check_output(args, shell=True)

    def get_conn(self):
        return pyodbc.connect(self._get_connection_string())

    def execute_scalar(self, sql: str):
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def get_one_result(self, sql: str):
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(sql)
            if cur:
                return cur.fetchall()[0][0]
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cur.close()



    def get_query_results(self, sql: str):
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(sql)
            return cur
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cur.close()

    def execute_sql(self, sql: str):
        print(sql)
        conn = self.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(sql)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cur.close()

    def execute_multiple_sql_statements(self, sql: []):
        from StringUtil import StringUtil
        if not sql:
            return

        for s in sql:
            print(s)
            conn = self.get_conn()
            cur = conn.cursor()
            try:
                cur.execute(s)
            except Exception as e:
                conn.rollback()
                raise e

        conn.commit()
        conn.close()



    def find_missing_objects_in_list(self, objects):
        if not objects:
            return

        if type(objects) not in [tuple, list]:
            raise ValueError('Expecting a list of objects, but found type {}'.format(type(objects)))

        sql = """
            SELECT CONCAT(schema_name(schema_id), '.', name) as name
            FROM sys.objects
            WHERE
                CONCAT(schema_name(schema_id), '.', name) IN (
        """

        for i, o in enumerate(objects):
            if i > 0:
                sql += ', '

            sql += "'{}'".format(o)

        sql += ')'

        cur = self.get_query_results(sql)

        objects_found = [x[0] for x in cur]
        missing_objects = []

        if len(objects_found) == len(objects):
            return []
        else:
            for o in objects:
                if str(o).strip().lower() not in objects_found:
                    missing_objects.append(o)

        return missing_objects




    @staticmethod
    def read_script(path: str,
                    is_remove_use_database_statement: bool = False,
                    is_return_list: bool=False):

        from StringUtil import StringUtil
        if not path:
            return

        try:
            lines = []
            with codecs.open(path, 'r', encoding='utf-8') as f:
                for line in f.readlines():
                    if is_remove_use_database_statement:
                        if line and (line.strip().lower().startswith('use') or ord(line.strip().lower()[0]) == 65279):
                            continue
                    chars = b''
                    lines.append(line)
            if not is_return_list:
                return ''.join(lines)
            else:
                return lines
        except Exception as e:
            raise type(e)('{}{}'.format(e, ' Unable to read file: {}'.format(path)))

    @staticmethod
    def get_all_sql_from_path(path: str,
                              is_remove_use_database_statement: bool = False):
        if not path:
            return

        sql = []
        for file in os.listdir(path):
            if not file.strip().lower().endswith('.sql'):
                continue
            sql.append(MssqlDatabase.read_script(path=os.path.join(path, file),
                                                 is_remove_use_database_statement=is_remove_use_database_statement,
                                                 is_return_list=False))

        return sql

    @staticmethod
    def remove_use_database_from_sql_script(script: str, database: str):
        if not script or not database:
            return

        import re

        regex = re.compile('use\s+\[?{}\]?'.format(database), re.IGNORECASE)
        return regex.sub('', script)

    @staticmethod
    def get_files_from_database_backup_dir(path: str):
        if not path:
            return

        if not os.path.exists(path):
            raise ValueError('path not found: {}'.format(path))


    def __get_subfolders(self):
        return self.__sub_folders.keys()

    def __make_dirs(self):
        """
        Make all the folders defined in __sub_folders
        This is done on initialization

        :return:
        :rtype:
        """
        for folder in self.__get_subfolders():
            path = os.path.join(self.path, folder)
            if not os.path.exists(path):
                self.logger.info('Making path {}'.format(path))
                try:
                    os.makedirs(path, exist_ok=True)
                except Exception as e:
                    self.logger.critical('Unable to make path {}\n{}'.format(path, e))
                    exit(1)

    def __create_path(self, path: str):
        """
        Utility function for creating parent directories if they don't exist
        :param path:
        :type path:
        :return:
        :rtype:
        """
        if not path or path.strip() == '':
            return

        if not os.path.exists(path):
            try:
                self.logger.info('Creating path: {}'.format(path))
                os.makedirs(path, exist_ok=True)
            except Exception as e:
                self.logger.error('Failed to create path {}\n{}'.format(path, e))

    def __get_folder_by_mssqlscript_object_type(self,
                                                object_type: MssqlScripterObjectType,
                                                is_abspath: bool = True):
        """
        Script out each type of objects to their appropriate folder
        Objects and folders defined in __sub_folders

        :param object_type: type of object (Enum)
        :type object_type:
        :param is_abspath: whether to build the absolute path when scripting the objects
        :type is_abspath:
        :return:
        :rtype: void
        """
        self.__make_dirs()

        path = ''
        for k, v in self.__sub_folders.items():
            if MssqlScripterObjectType(v).value == object_type.value:
                path = k

        if not path:
            self.logger.error('Unable to map MssqlScript object to path for object: {}'.format(object_type))
            return

        if is_abspath:
            return os.path.join(self.path, path)
        else:
            return path


    def __get_default_mssqlscripter_args(self):
        """
        Gets the arguments for the connection information
        This is used by all calls

        :return:
        :rtype:
        """
        return [
            'mssql-scripter',
            MssqlScripterArguments.SERVER.value,
            self.server,
            MssqlScripterArguments.DATABASE.value,
            self.database,
            MssqlScripterArguments.USER.value,
            self.username,
            MssqlScripterArguments.PASSWORD.value,
            self.password,
            MssqlScripterArguments.DISPLAY_PROGRESS.value
        ]

    def __get_mssqlscripter_args(self,
                                 is_add_default_options: bool = True,
                                 is_script_create: bool = False,
                                 is_script_drop_create: bool = False,
                                 is_file_per_object: bool = False,
                                 file_path: str = None,
                                 include_types: [] = None,
                                 exclude_types: [] = None,
                                 include_objects: [] = None,
                                 exclude_objects: [] = None,
                                 is_check_for_existence: bool = False,
                                 is_change_tracking: bool = False,
                                 is_data_only: bool = False,
                                 is_schema_and_data: bool = False,
                                 is_append: bool = False):
        """
        Helper function to generate a list of arguments to pass to mssql-scripter

        :param is_add_default_options:
        :type is_add_default_options:
        :param is_script_create:
        :type is_script_create:
        :param is_script_drop_create:
        :type is_script_drop_create:
        :param is_file_per_object:
        :type is_file_per_object:
        :param file_path:
        :type file_path:
        :param include_types:
        :type include_types:
        :param exclude_types:
        :type exclude_types:
        :return:
        :rtype:
        """

        args = []

        if is_add_default_options:
            for opt in self.__default_mssqlscripter_options:
                args.append(opt)

        if is_script_create and not is_script_drop_create:
            args.append(MssqlScripterArguments.SCRIPT_CREATE.value)

        if is_script_drop_create:
            args.append(MssqlScripterArguments.SCRIPT_DROP_CREATE.value)
            args.append(MssqlScripterArguments.CHECK_FOR_EXISTENCE.value)

        if is_file_per_object:
            args.append(MssqlScripterArguments.FILE_PER_OBJECT.value)

        if file_path and not file_path.strip() == '':
            path = ''
            if os.path.abspath(path).strip().lower() != path.strip().lower():
                path = os.path.join(self.local_path, file_path)
            else:
                path = file_path

            if not os.path.exists(path):
                self.__create_path(path)

            args.append(MssqlScripterArguments.FILE_PATH.value)
            args.append(path)

        if include_types:
            try:
                args.append(MssqlScripterArguments.INCLUDE_TYPES.value)

                if type(include_types) in [tuple, list]:
                    for t in include_types:
                        args.append(t)
                elif type(include_types) == str:
                    args.append(include_types)
                else:
                    raise TypeError('Type for included types is not correct: {}'.format(type(include_types)))
            except Exception as e:
                self.logger.error('Unable to add included types: {}'.format(e))
                if MssqlScripterArguments.INCLUDE_TYPES.value in args:
                    args.remove(MssqlScripterArguments.INCLUDE_TYPES.value)

        if exclude_types:
            try:
                args.append(MssqlScripterArguments.EXCLUDE_TYPES.value)

                if type(exclude_types) in [tuple, list]:
                    for t in exclude_types:
                        args.append(t)
                elif type(exclude_types) == str:
                    args.append(exclude_types)
                else:
                    raise TypeError('Unable to add excluded types: {}'.format(type(exclude_types)))
            except Exception as e:
                self.logger.error('Unable to add excluded types: {}'.format(e))
                if MssqlScripterArguments.EXCLUDE_TYPES.value in args:
                    args.remove(MssqlScripterArguments.EXCLUDE_TYPES.value)

        if include_objects:
            try:
                args.append(MssqlScripterArguments.INCLUDE_OBJECTS.value)

                if type(include_objects) in [tuple, list]:
                    for t in include_objects:
                        args.append(t)
                elif type(include_objects) == str:
                    args.append(include_objects)
                else:
                    raise TypeError('Unable to add included object. Bad type: {}'.format(type(include_objects)))
            except Exception as e:
                self.logger.error('Unable to add included objects: {}'.format(e))
                if MssqlScripterArguments.INCLUDE_TYPES.value in args:
                    args.remove(MssqlScripterArguments.INCLUDE_TYPES.value)

        # TODO: Add this functionality in: be able to exclude objects
        if exclude_objects:
            raise NotImplementedError

        if is_check_for_existence:
            args.append(MssqlScripterArguments.CHECK_FOR_EXISTENCE.value)

        if is_change_tracking:
            args.append(MssqlScripterArguments.CHANGE_TRACKING.value)

        if is_data_only:
            args.append(MssqlScripterArguments.DATA_ONLY.value)

        if is_schema_and_data:
            args.append(MssqlScripterArguments.SCHEMA_AND_DATA.value)

        if is_append:
            args.append(MssqlScripterArguments.APPEND.value)

        return args

    def _get_arguments_for_list_args(self, list_arg, arg_type: str):
        if not list_arg or not arg_type or len(list_arg) == 0:
            return

        if arg_type not in [a.value for a in MssqlScripterArguments]:
            raise Exception('Invalid value for arg_type: {}'.format(arg_type))

        args = []
        args.append(arg_type)

        if type(list_arg) in [tuple, list]:
            for t in list_arg:
                args.append(t)
        elif type(list_arg) == str:
            args.append(list_arg)
        else:
            raise TypeError('Invalid type for list_arg parameter {}'.format(type(list_arg)))

        return args

    def _add_list_args_to_args(self, current_list: [], new_list: []):
        if not current_list or not new_list:
            return

        for arg in new_list:
            current_list.append(arg)

        return current_list

    def __do_mssqlscripter_action(self, args, is_include_default_options: bool = True):
        from platform import system as platform_system

        """
        Execute a command to mssql-scripter
        Args does not need to include connection information

        :param args:
        :type args:
        :param is_include_default_options:
        :type is_include_default_options:
        :return:
        :rtype:
        """
        try:
            if type(args) not in [tuple, list, dict]:
                raise TypeError('Arguments must type of be list, tuple, or dict')

            all_args = self.__get_default_mssqlscripter_args()

            if is_include_default_options:
                current_args = [x.strip().lower() for x in args]

                for arg in self.__default_mssqlscripter_options:
                    if arg.strip().lower() not in current_args:
                        all_args.append(arg)

            if type(args) in [tuple, list]:
                for arg in args:
                    all_args.append(arg)

            if type(args) == dict:
                for k, v in args:
                    all_args.append(k)
                    all_args.append(v)

            self.logger.info('Executing command: {}'.format(all_args))
            print(' '.join(all_args))

            if platform_system().lower() == 'windows':
                return subprocess.check_output(all_args, shell=True)  # For Windows: shell=True
            else:
                return subprocess.check_output(all_args)

        except Exception as e:
            self.logger .error('Unable to perform mssql-scripter action: {}\nargs: {}'.format(e, args))

    def get_objects_not_found(self, objects: []):
        if not objects:
            return

        bad_objects = []
        for o in objects:
            # sql = "SELECT ObjectId = OBJECT_ID('{}')".format(o)
            sql = """
                ;WITH c AS (
                    SELECT 
                        CONCAT(s.name, '.', o.name) AS FullName 
                    FROM sys.objects o
                    JOIN sys.schemas s ON o.schema_id = s.schema_id
                )
                
                SELECT ISNULL(
                        (SELECT TOP 1 1
                        FROM c 
                        WHERE 
                            REPLACE(REPLACE('{}', '[', ''), ']', '') = c.FullName)
                , 0) AS IsExists """.format(o)
            object_Id = self.get_one_result(sql)
            if not object_Id:
                bad_objects.append(o)

        if bad_objects or len(bad_objects) > 0:
            self.logger.warning(
                'Some objects cannot be found in the source database')
            for obj in bad_objects:
                self.logger.warning('\t{}'.format(obj))
            return bad_objects
        else:
            return []

    def script_objects(self,
                       objects: [],
                       path: str=None,
                       is_file_per_object: bool=True):
        from StringUtil import StringUtil

        if not objects:
            return

        if path and not os.path.exists(path):
            self.logger.error('Path does not exist {}'.format(path))
            raise ValueError

        invalid_objects = self.get_objects_not_found(objects)
        if invalid_objects and len(invalid_objects) > 0:
            self.logger.error('Unable to find all objects')
            raise ValueError

        args = []
        if path:
            args.append(MssqlScripterArguments.FILE_PATH.value)
            args.append(path)

        if is_file_per_object and path:
            args.append(MssqlScripterArguments.FILE_PER_OBJECT.value)

        args.append(MssqlScripterArguments.INCLUDE_OBJECTS.value)
        for o in objects:
            args.append(o)

        args.append(MssqlScripterArguments.EXCLUDE_USE_DATABASE.value)
        args.append(MssqlScripterArguments.SCRIPT_DROP_CREATE.value)
        args.append(MssqlScripterArguments.CHECK_FOR_EXISTENCE.value)

        result = self.__do_mssqlscripter_action(args, is_include_default_options=True)


        if not path:
            output_str = result.decode('ascii', 'ignore')

            return''.join(output_str)

    def compare_objects_in_databases(self, db1: str, db2: str):
        if not db1 or not db2:
            return


        sql = """
        ;WITH c_DB1_GetData AS (
            SELECT
                o.object_id
                ,FullName = CONCAT(s.name, '.', o.name)
                ,FullNameQuoted  = CONCAT(QUOTENAME(s.name), '.', QUOTENAME(o.name))
                ,o.name
                ,o.[type]
                ,o.type_desc
                ,o.create_date
                ,o.modify_date
                ,ObjDefinition = REPLACE(REPLACE(TRIM(m.definition), CHAR(10), ''), CHAR(13), '')
            FROM {0}.sys.objects o
            JOIN {0}.sys.schemas s ON o.schema_id = s.schema_id
            LEFT JOIN {0}.sys.sql_modules m ON o.object_id = m.object_id
            WHERE 
                o.type_desc IN (
                    'SQL_SCALAR_FUNCTION'
                    ,'SQL_INLINE_TABLE_VALUED_FUNCTION'
                    ,'VIEW'
                    ,'SQL_STORED_PROCEDURE'
                )
        )
        ,c_DB2_GetData AS (
            SELECT
                o.object_id
                ,FullName = CONCAT(s.name, '.', o.name)
                ,FullNameQuoted  = CONCAT(QUOTENAME(s.name), '.', QUOTENAME(o.name))
                ,o.name
                ,o.[type]
                ,o.type_desc
                ,o.create_date
                ,o.modify_date
                --,ObjDefinition = REPLACE(REPLACE(TRIM(OBJECT_DEFINITION(o.object_id)), CHAR(10), ''), CHAR(13), '')
                ,ObjDefinition = REPLACE(REPLACE(TRIM(m.definition), CHAR(10), ''), CHAR(13), '')
            FROM {1}.sys.objects o
            JOIN {1}.sys.schemas s ON o.schema_id = s.schema_id
            LEFT JOIN {1}.sys.sql_modules m ON o.object_id = m.object_id
            WHERE 
                o.type_desc IN (
                    'SQL_SCALAR_FUNCTION'
                    ,'SQL_INLINE_TABLE_VALUED_FUNCTION'
                    ,'VIEW'
                    ,'SQL_STORED_PROCEDURE'
                )
        )
        ,c_DB1 AS (
            SELECT
                Sha1Hash = HASHBYTES('SHA1', c.ObjDefinition)
                ,*
            FROM c_DB1_GetData c
        )
        ,c_DB2 AS (
            SELECT
                Sha1Hash = HASHBYTES('SHA1', c.ObjDefinition)
                ,*
            FROM c_DB2_GetData c
        )
        ,c_NotExists AS (
            SELECT
                db1.*
            FROM c_DB1 db1
            LEFT JOIN c_DB2 db2 ON db1.FullNameQuoted = db2.FullNameQuoted
            WHERE 
                db2.FullNameQuoted IS NULL 
        )
        ,c_Hash AS (
        
            SELECT 
                -- CompareType = 'hash'
                db1.*
            FROM c_DB1 db1
            LEFT JOIN c_DB2 db2 ON db1.Sha1Hash = db2.Sha1Hash
            WHERE 
                db2.object_id IS NULL 
        )
        
        SELECT *
        FROM (
            SELECT 
                CompareType = 'not exists'
                ,*
            FROM c_NotExists c
            UNION 
            SELECT
                CompareType = 'hash'
                ,*
            FROM c_Hash c
            WHERE NOT EXISTS (
                SELECT 1 
                FROM c_NotExists 
                WHERE 
                    c.FullNameQuoted = FullNameQuoted
            )
        ) v 
        ORDER BY 
            v.CompareType
            ,v.FullNameQuoted
        """.format(db1, db2)

        return self.get_rows_from_sql(sql)





