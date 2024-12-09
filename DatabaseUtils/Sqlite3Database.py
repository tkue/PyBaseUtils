import logging
import os
import sqlite3

from . import Database
from . import DatabaseType


class Sqlite3Database(Database):

    def __init__(self,
                 database_path: str,
                 logger: logging.Logger,
                 schema_script_path: str = None,
                 setup_scripts: [] = None,
                 is_force_new_initialization: bool = False):
        super(Sqlite3Database, self).__init__(database_type=DatabaseType.SQLITE3,
                                              logger=logger)

        self.database_path = database_path
        self.logger = logger
        self.schema_script_path = schema_script_path
        self.setup_scripts = setup_scripts
        self.is_force_new_initialization = is_force_new_initialization

        self.__initialize_database(self.is_force_new_initialization)

    def __initialize_database(self, is_force_new_initialization: bool = False):
        """
        Setup schema, insert data, etc.

        By default, won't initialize if the database already exists (unless forced)
        :param is_force_new_initialization: Do we want to force initialization and run all the scripts
        :type is_force_new_initialization:
        :return:
        :rtype:
        """
        self.logger.info('Initializing database')

        # Check if we should initialize database with scripts
        if not is_force_new_initialization and os.path.isfile(self.database_path):
            self.logger.info('Database exists. Skipping initialization')
            return

        # Schema
        if self.schema_script_path:
            self.logger.info('Executing schema script')
            self.execute_sql_script(self.schema_script_path)
        else:
            self.logger.info('No schema script to execute')

        # Setup scripts
        if self.setup_scripts:
            self.logger.info('Executing setup scripts')
            self.execute_sql_scripts(self.setup_scripts)
        else:
            self.logger.info('No setup scripts to execute')

    @staticmethod
    def get_database_name(name: str):
        return os.path.basename(name)

    @staticmethod
    def get_database_abs_path(path: str):
        return os.path.abspath(path)

    @staticmethod
    def get_sql_commands_from_script(script_path: str):
        """
        sqlite3 library offers execute_script()
            This will only execute one command at a time
        This reads the script and returns an array of every command
        Commands are separated by semi-colon (';')

        :param script_path: path to SQL script
        :return: Array of SQL commands
        """
        if not script_path:
            return

        script = []

        with open(script_path, 'r') as f:
            file = f.readlines()

        command = ''
        for line in file:
            line = line.strip()
            if not line:
                continue

            if line.endswith(';'):
                command += line
                script.append(command)
                command = ''
            else:
                command += line
                command += '\n'

        return script

    def get_conn(self):
        return sqlite3.connect(self.database_path)

    def execute_sql_script(self, script_path: str):
        """
        Read a SQL script file and execute all commands
        :param script_path:
        :type script_path:
        :return:
        :rtype: void
        """
        self.logger.info('Executing script: {0}'.format(script_path))

        if not script_path:
            self.logger.error('Script path null')
            return

        if not os.path.isfile(script_path):
            self.logger.error('Script path is not a valid file: {0}'.format(script_path))
            return

        conn = self.get_conn()
        cur = conn.cursor()

        for command in Sqlite3Database.get_sql_commands_from_script(script_path):
            try:
                cur.execute(command)
                conn.commit()
            except sqlite3.Error as e:
                conn.rollback()
                self.logger.error('Failed to execute command:\n{0}\nError:\n{1}'.format(command, e))

    def is_exists(self, sql_statement: str):
        if not sql_statement:
            return

        conn = self.get_conn()
        try:
            cur = conn.cursor()

            cur.execute(sql_statement)

            if cur.fetchall() and len(cur.fetchall()) > 0:
                return True
        except:
            pass
        finally:
            conn.close()

        return False

    def execute_sql_scripts(self, script_paths: []):
        if not script_paths:
            return

        for path in script_paths:
            try:
                if not os.path.isfile(path):
                    raise FileNotFoundError(path)

                self.execute_sql_script(path)
            except Exception as e:
                self.logger.error('Unable to execute script: {}\n{}'.format(path, e))

    def get_one_result(self, query: str, params: ()):
        conn = self.get_conn()
        try:
            cur = conn.cursor()
            row = cur.execute(query, params).fetchone()[0]

            if not row:
                return None

            return row
        except Exception as e:
            # self.logger.error('Unable to get single result: \nQuery: {}\nparmas: {}\nException: {}'.format(query, params, e))
            return None
        finally:
            conn.close()
