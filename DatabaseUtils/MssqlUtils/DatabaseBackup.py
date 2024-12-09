import os
import subprocess

from logging import Logger

from DatabaseUtils.MssqlUtils.SqlObjectType import MssqlScripterObjectType
from DatabaseUtils.MssqlUtils.MssqlScripterOptions import MssqlScripterArguments
# from DatabaseUtils.Database import MssqlDatabase

from DatabaseUtils.MssqlUtils import Config


class DatabaseBackup(object):
    def __int__(self,
                path: str):
        self.path = path


class MssqlDatabaseBackup(DatabaseBackup):
    # from DatabaseUtils.MssqlUtils.Database import MssqlDatabase
    from DatabaseUtils.Database import MssqlDatabase
    # TODO: Map to multiple types
    __sub_folders = {
        'database': MssqlScripterObjectType.DATABASE,
        'tables': MssqlScripterObjectType.TABLE,
        'views': MssqlScripterObjectType.VIEW,
        'functions': MssqlScripterObjectType.USER_DEFINED_FUNCTION,
        'procedures': MssqlScripterObjectType.STORED_PROCEDURE
    }

    __default_mssqlscripter_options = [
        MssqlScripterArguments.EXCLUDE_HEADERS.value
    ]

    __path_for_other_objects = 'other'

    def __init__(self,
                 path: str,
                 database: MssqlDatabase,
                 logger: Logger):
        self.__logger = logger
        self.__database = database
        self.path = path

        # Initialize
        self.__make_dirs()

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, value):
        try:
            if not value or value.strip() == '':
                raise ValueError('Backup value cannot be empty')

            # Path must have database name in it
            path_parts = os.path.split(value)
            db = self.__database.database
            host = self.__database.server
            if not path_parts[len(path_parts) - 1].strip().lower() == db.strip().lower():
                full_path = os.path.join(value, '{}_{}'.format(host, db))
            else:
                full_path = value

            value = full_path

            if not os.path.exists(value):
                self.__logger.info('Making path: {}'.format(value))
                try:
                    os.makedirs(value, exist_ok=True)
                except Exception as e:
                    self.__logger.critical('Unable to make backup path: {}'.format(e))
                    exit(1)
        except Exception as e:
            self.__logger.critical('Unable to create base path for the backup for given path {}: {}'.format(value, e))
            exit(1)

        self._path = os.path.abspath(value)

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
                self.__logger.info('Making path {}'.format(path))
                try:
                    os.makedirs(path, exist_ok=True)
                except Exception as e:
                    self.__logger.critical('Unable to make path {}\n{}'.format(path, e))
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
                self.__logger.info('Creating path: {}'.format(path))
                os.makedirs(path, exist_ok=True)
            except Exception as e:
                self.__logger.error('Failed to create path {}\n{}'.format(path, e))

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
            self.__logger.error('Unable to map MssqlScript object to path for object: {}'.format(object_type))
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
            self.__database.server,
            MssqlScripterArguments.DATABASE.value,
            self.__database.database,
            MssqlScripterArguments.USER.value,
            self.__database.username,
            MssqlScripterArguments.PASSWORD.value,
            self.__database.password,
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
                path = os.path.join(self._path, file_path)
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
                self.__logger.error('Unable to add included types: {}'.format(e))
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
                self.__logger.error('Unable to add excluded types: {}'.format(e))
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
                self.__logger.error('Unable to add included objects: {}'.format(e))
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

            self.__logger.info('Executing command: {}'.format(all_args))
            print(' '.join(all_args))

            if platform_system().lower() == 'windows':
                return subprocess.check_output(all_args, shell=True)  # For Windows: shell=True
            else:
                return subprocess.check_output(all_args)

        except Exception as e:
            self.__logger.error('Unable to perform mssql-scripter action: {}\nargs: {}'.format(e, args))

    def __get_log_name(self):
        return Config.get_log_filename()

    def script_all_objects(self,
                           path: str = None,
                           is_script_drop_create: bool = True,
                           is_file_per_object: bool = True):
        args = []

        # Default is drop/create - easier to deploy from code
        if is_script_drop_create:
            args.append(MssqlScripterArguments.SCRIPT_DROP_CREATE.value)
            args.append(MssqlScripterArguments.CHECK_FOR_EXISTENCE.value)
        else:
            args.append(MssqlScripterArguments.SCRIPT_CREATE.value)

        # Do we want to create one file per object?
        if is_file_per_object:
            args.append(MssqlScripterArguments.FILE_PER_OBJECT.value)

        if not path or path.strip() == '':
            path = self._path
        else:
            self.__create_path(path)

        args.append(MssqlScripterArguments.FILE_PATH.value)
        args.append('{}'.format(path))

        return self.__do_mssqlscripter_action(args)

    def __script_objects_by_type_to_path(self,
                                         object_type: MssqlScripterObjectType,
                                         is_script_drop_create: bool = True,
                                         is_file_per_object: bool = True,
                                         include_objects: [] = None):
        """
        Pass a specific object type (Enum) and script out those objects to the appropriate path
        Objects and their paths are defined in __sub_folders

        :param object_type:
        :type object_type:
        :param is_script_drop_create:
        :type is_script_drop_create:
        :param is_file_per_object:
        :type is_file_per_object:
        :return:
        :rtype:
        """
        path = self.__get_folder_by_mssqlscript_object_type(object_type)

        if not path:
            self.__logger.error('Unable to map path for object: {}'.format(object_type))
            return

        path = os.path.join(self.path, path)

        args = self.__get_mssqlscripter_args(is_add_default_options=True,
                                             is_script_drop_create=is_script_drop_create,
                                             is_file_per_object=is_file_per_object,
                                             file_path=path,
                                             include_types=object_type.value,
                                             include_objects=include_objects)
        self.__do_mssqlscripter_action(args=args, is_include_default_options=True)
    def __get_sql_full_scripted_database(self):
        # path = '{}.{}'.format(self.__database.database, 'sql')

        print('one')
        args = self.__get_mssqlscripter_args(is_add_default_options=True,
                                             is_script_drop_create=True,
                                             is_file_per_object=False,
                                             file_path=None,
                                             include_types=None,
                                             exclude_types=None,
                                             include_objects=None,
                                             exclude_objects=None,
                                             is_check_for_existence=True,
                                             is_change_tracking=True,
                                             is_data_only=False,
                                             is_schema_and_data=False,
                                             is_append=False)
        return self.__do_mssqlscripter_action(args=args, is_include_default_options=True)

    # TODO: when getting last run time, get the time and not just the date
    def __get_last_run_time(self):
        if not os.path.isfile(self.__get_log_name()):
            return None

        log_file = self.__get_log_name()

        log_date = None
        try:
            with open(log_file, 'r') as f:
                for line in f.readlines():
                    if not line.find('mssql-scripter'):
                        continue
                    if log_date:
                        break

                    log_date = line.split(' ')[0]
        except Exception as e:
            self.__logger.error('Unable to parse date from log file {}:\n{}'.format(log_file, e))

        return log_date

    def __get_list_of_objects(self, from_date: str = None):
        return self.__database.get_objects(from_date, as_dict=True)

    def script_other_objects(self,
                             path: str,
                             is_script_drop_create: bool = True,
                             is_file_per_object: bool = True,
                             included_objects: [] = None):
        """
        This scripts any object whose type is not defined in __sub_folders

        :param path:
        :type path:
        :param is_script_drop_create:
        :type is_script_drop_create:
        :param is_file_per_object:
        :type is_file_per_object:
        :return:
        :rtype:
        """
        try:
            if not path:
                raise ValueError('Path cannot be null for scripting other objects')

            path = os.path.abspath(os.path.join(self._path, path))

            args = self.__get_mssqlscripter_args(file_path=path,
                                                 is_script_drop_create=is_script_drop_create,
                                                 is_file_per_object=is_file_per_object,
                                                 exclude_types=[v.value for k, v in self.__sub_folders.items()],
                                                 include_objects=included_objects,
                                                 is_check_for_existence=True,
                                                 is_change_tracking=True,
                                                 is_data_only=False,
                                                 is_schema_and_data=False,
                                                 is_append=False
                                                 )
            self.__do_mssqlscripter_action(args=args, is_include_default_options=True)
        except Exception as e:
            self.__logger.error('Unable to script other objects: {}'.format(e))

    def script_objects_to_folders(self,
                                  include_objects: [] = None):
        """
        For each path and object type in __sub_folders,
            script them to their folder

        :return:
        :rtype:
        """
        # TODO: Be able to script multiple types
        for k, v in self.__sub_folders.items():
            self.__script_objects_by_type_to_path(object_type=v,
                                                  is_script_drop_create=True,
                                                  is_file_per_object=True,
                                                  include_objects=include_objects)

    def do_full_backup(self,
                       is_changed_objects_only: bool = True,
                       to_file_path: bool = True):
        """
        1) Script out all object types in __sub_folders to their corresponding folder
        2) Script out all object types not defined in __sub_folders

        :return:
        :rtype:
        """

        if is_changed_objects_only:
            include_objects = self.get_changes_from_last_run()
            if len(include_objects) > 0:
                self.script_objects_to_folders(include_objects=include_objects)
                self.script_other_objects(path=self.__path_for_other_objects, included_objects=include_objects)
            else:
                self.__logger.warning('No object changes')
        else:
            self.script_objects_to_folders()
            self.script_other_objects(path=self.__path_for_other_objects)

    def get_script_objects(self, object_names: [], file_path: str=None):
        from StringUtil import StringUtil
        from DatabaseUtils import Database
        from DirectoryCollection import DirectoryCollection

        if not object_names:
            return
        file_path = os.path.splitext(file_path)[0]

        existing_items_in_path = DirectoryCollection.get_files_and_hashes(file_path, False, True)


        args = self.__get_mssqlscripter_args(is_add_default_options=True,
                                             is_script_drop_create=True,
                                             is_file_per_object=True,
                                             file_path=file_path,
                                             include_types=[],
                                             include_objects=object_names)

        self.__do_mssqlscripter_action(args=args, is_include_default_options=True)

        new_items_in_path = DirectoryCollection.get_files_and_hashes(file_path, False, True)

        outname = '{}_{}_{}-Objects_{}.sql'.format(self.__database.server,
                                               self.__database.database,
                                               len(new_items_in_path),
                                               StringUtil.get_timestamp_abbr())
        all_sql = []
        os.chdir(file_path)
        for script in [x for x in os.listdir(os.curdir) if os.path.isfile(x)]:
            all_sql.append([x for x in Database.MssqlDatabase.read_script(script, is_return_list=True)])
            print(Database.MssqlDatabase.read_script(script))
            all_sql.append('\nGO\n')

        with open(outname, 'w') as f:
            for line in all_sql:
                f.write('{}\n'.format(line))






    def get_changes_from_last_run(self):
        o = self.__get_list_of_objects(self.__get_last_run_time())
        changed_objects = []
        for obj in o:
            changed_objects.append(obj['FullName'])

        return changed_objects




    def get_scripted_database(self):
        args = self.__get_mssqlscripter_args(is_add_default_options=True,
                                             is_script_drop_create=True,
                                             is_file_per_object=False,
                                             file_path=None,
                                             include_types=None,
                                             exclude_types=None,
                                             include_objects=None,
                                             exclude_objects=None,
                                             is_check_for_existence=True,
                                             is_change_tracking=True,
                                             is_data_only=False,
                                             is_schema_and_data=False,
                                             is_append=False)
        script = self.__do_mssqlscripter_action(args=args, is_include_default_options=True).decode('ascii', 'ignore')
        script = MssqlDatabase.remove_use_database_from_sql_script(script, self.__database.database)
        path = os.path.join(self.path, '{}.{}'.format(self.__database.database, 'sql'))
        print(path)
        print(script)
        try:
            with open(path, 'w+') as f:
                f.write(script)
        except Exception as e:
            self.__logger.error('Failed to create scripted database backup: {}'.format(e))


