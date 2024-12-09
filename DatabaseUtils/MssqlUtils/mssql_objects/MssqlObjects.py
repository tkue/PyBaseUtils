from enum import Enum
from abc import ABCMeta, abstractmethod, abstractproperty


class MssqlObjectType(Enum):
    AGGREGATE_FUNCTION = 'aggregate_function'
    CHECK_CONSTRAINT = 'check_constraint'
    CLR_SCALAR_FUNCTION = 'clr_scalar_function'
    CLR_STORED_PROCEDURE = 'clr_stored_procedure'
    CLR_TABLE_VALUED_FUNCTION = 'clr_table_valued_function'
    CLR_TRIGGER = 'clr_trigger'
    DEFAULT_CONSTRAINT = 'default_constraint'
    EXTENDED_STORED_PROCEDURE = 'extended_stored_procedure'
    FOREIGN_KEY_CONSTRAINT = 'foreign_key_constraint'
    INTERNAL_TABLE = 'internal_table'
    PLAN_GUIDE = 'plan_guide'
    PRIMARY_KEY_CONSTRAINT = 'primary_key_constraint'
    REPLICATION_FILTER_PROCEDURE = 'replication_filter_procedure'
    RULE = 'rule'
    SEQUENCE_OBJECT = 'sequence_object'
    SERVICE_QUEUE = 'service_queue'
    SQL_INLINE_TABLE_VALUED_FUNCTION = 'sql_inline_table_valued_function'
    SQL_SCALAR_FUNCTION = 'sql_scalar_function'
    SQL_STORED_PROCEDURE = 'sql_stored_procedure'
    SQL_TABLE_VALUED_FUNCTION = 'sql_table_valued_function'
    SQL_TRIGGER = 'sql_trigger'
    SYNONYM = 'synonym'
    SYSTEM_TABLE = 'system_table'
    TABLE_TYPE = 'table_type'
    UNIQUE_CONSTRAINT = 'unique_constraint'
    USER_TABLE = 'user_table'
    VIEW = 'view'


# class SqlServerObjectType(Enum):
#     AGGREGATE_FUNCTION = 'aggregate_function'
#     CHECK_CONSTRAINT = 'check_constraint'
#     CLR_SCALAR_FUNCTION = 'clr_scalar_function'
#     CLR_STORED_PROCEDURE = 'clr_stored_procedure'
#     CLR_TABLE_VALUED_FUNCTION = 'clr_table_valued_function'
#     CLR_TRIGGER = 'clr_trigger'
#     DEFAULT_CONSTRAINT = 'default_constraint'
#     EXTENDED_STORED_PROCEDURE = 'extended_stored_procedure'
#     FOREIGN_KEY_CONSTRAINT = 'foreign_key_constraint'
#     INTERNAL_TABLE = 'internal_table'
#     PLAN_GUIDE = 'plan_guide'
#     PRIMARY_KEY_CONSTRAINT = 'primary_key_constraint'
#     REPLICATION_FILTER_PROCEDURE = 'replication_filter_procedure'
#     RULE = 'rule'
#     SEQUENCE_OBJECT = 'sequence_object'

class SqlServerTableType(Enum):
    REAL_TABLE = 0
    LOCAL_TEMP_TABLE = 1
    GLOBAL_TEMP_TABLE = 2

class DatabaseObject(object):

    def __init__(self,
                 mssql_object_type: MssqlObjectType):
        self.mssql_object_type = mssql_object_type


class SqlServerObject(DatabaseObject):
    def __init__(self,
                 name: str,
                 schema: str,
                 database: str,
                 sql_server_object: MssqlObjectType):
        super(SqlServerObject, self).__init__(sql_server_object)
        self._name = name
        self._schema = schema
        self._database = database

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, val):
        # self.name = val
        self._name = SqlServerObject.unquote_value(val)

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, val):
        self._schema = SqlServerObject.unquote_value(val)

    @property
    def database(self):
        return self._database

    @database.setter
    def database(self, val):
        self._database = SqlServerObject.unquote_value(val)

    @staticmethod
    def is_value_quoted(val: str):
        if not val:
            return False

        val = val.strip()

        if val.startswith('[') and val.endswith(']'):
            return True
        else:
            return False

    @staticmethod
    def unquote_value(val: str):
        if not val:
            return

        val = val.strip()

        if SqlServerObject.is_value_quoted(val):
            return val[1:len(val) - 1]
        else:
            return val

    @staticmethod
    def get_quoted_name(val: str):
        if not val:
            return

        if SqlServerObject.is_value_quoted(val):
            return val
        else:
            return SqlServerObject.unquote_value(val)

    def get_object_full_name(self,
                             is_quoted: bool,
                             is_include_database_name: bool = False):

        name = self.name
        schema = self.schema
        database = ''

        if is_include_database_name:
            database = self.database

        if is_quoted:
            name = SqlServerObject.get_quoted_name(name)
            schema = SqlServerObject.get_quoted_name(schema)

            if is_include_database_name:
                database = SqlServerObject.get_quoted_name(database)

        schema_name = '{}.{}'.format(schema, name)

        if is_include_database_name:
            return '{}.{}'.format(database, schema_name)
        else:
            return schema_name


class Column(object):
    def __init__(self,
                 name,
                 sql,
                 is_nullable: bool = True):
        self._name = name
        self._sql = sql
        self._is_nullable = is_nullable


    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, val):
        if not val:
            return

        self._name = SqlServerObject.unquote_value(val.strip())

    @property
    def sql(self):
        return self._sql

    @sql.setter
    def sql(self, val):
        if not val:
            return

        self._sql = val

    @property
    def is_nullable(self):
        return self._is_nullable

    @is_nullable.setter
    def is_nullable(self, val):
        if not val:
            return

        self._is_nullable = val

    @staticmethod
    def unquote_name(val: str):
        if not val:
            return

        if val.startswith('[') and val.endswith(']'):
            return val[1:len(val) - 1]
        else:
            return val

        # column_name = SqlServerObject.unquote_value(column_name)

    def get_sql(self, is_quoted: bool):
        col_name = self.name
        if is_quoted:
            col_name = '[{}]'.format(col_name)

        sql = '{} {}'.format(col_name, self.sql)

        if self.is_nullable:
            return '{} NOT NULL'.format(sql)
        else:
            return sql


class Table(SqlServerObject):
    def __init__(self,
                 name,
                 schema,
                 database,
                 columns: []):
        super(Table, self).__init__(name=name,
                                    schema=schema,
                                    database=database,
                                    sql_server_object=MssqlObjectType.USER_TABLE)
        self.columns = columns

    def _get_sql_for_create_table(self, pound_signs: str = None):
        if not pound_signs:
            pound_signs = ''
        return 'CREATE TABLE {}{} ('.format(pound_signs, self.get_object_full_name(is_quoted=True))

    def _get_sql_for_create_local_temp_table(self):
        return self._get_sql_for_create_table('#')

    def _get_sql_for_create_global_temp_table(self):
        return self._get_sql_for_create_table('##')

    def get_sql_create_table(self,
                             is_local_temp_table: bool = False,
                             is_global_temp_table: bool = False,
                             included_column_names: [] = None,
                             excluded_column_names: [] = None,
                             additional_columns: [] = None):
        sql = ''
        if is_global_temp_table:
            sql = self._get_sql_for_create_global_temp_table()
        elif is_local_temp_table:
            sql = self._get_sql_for_create_local_temp_table()
        else:
            sql = self._get_sql_for_create_table()

        # sql = 'CREATE TABLE {} ('.format(self.get_object_full_name(is_quoted=True))

        for i, col in enumerate(self.columns):
            if included_column_names and col.name not in included_column_names:
                continue
            if excluded_column_names and col.name in excluded_column_names:
                continue

            col_sql = '\n{}'.format(col.get_sql(True))

            # Add commas - this method puts it at the end
            if i > 0:
                col_sql = ',{}'.format(col_sql)

            sql += col_sql

        if additional_columns:
            for col in additional_columns:
                sql += '\n{}'.format(col.get_sql(True))

        sql += '\n)'

        return sql
