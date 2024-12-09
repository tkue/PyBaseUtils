from DatabaseUtils.Database import Database, MssqlDatabase, DatabaseType
from enum import Enum
import json
import logging
import subprocess
import os
from StringUtil import StringUtil
import argparse

class SoftwareBranchType(Enum):
    TRUNK = 'trunk'
    ALPHA = 'alpha'
    BETA = 'beta'
    STABLE = 'stable'

class DatabaseDeploymentConfig(object):
    def __init__(self, file_path: str):
        if not file_path:
            raise ValueError
        self._logger = logging.getLogger(__name__)

        dir_path = os.path.dirname(os.path.realpath(__file__))
        self._file_path = os.path.join(dir_path, file_path)

        self._config = self.__read_config()



    def __read_config(self):
        self._logger.info('Getting config at {}'.format(self._file_path))
        with open(self._file_path, 'r') as f:
            lines = f.readlines()

        return json.loads(' '.join(lines))

    def get_local_repo_path(self):
        return self._config['local_repo_path']

    def get_database(self,
                     alias: str):
        if not alias:
            return

        alias = alias.strip().lower()

        for db in self._config['databases']:
            if db['alias'].strip().lower() == alias:
                return MssqlDatabase(server=db['host'],
                                     database=db['database'],
                                     username=db['username'],
                                     password=db['password'],
                                     port=db['port'],
                                     local_path=db['local_path'])

    @staticmethod
    def get_logger():
        logger = logging.getLogger('DatabaseDeployment')
        logger.setLevel(logging.INFO)

        fh = logging.FileHandler('database_deployment.log')
        fh.setLevel(logging.INFO)

        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        logger.addHandler(fh)
        logger.addHandler(ch)

        return logger


class DatabaseDeploymentCollections(object):
    def __int__(self, args: []):
        deployment_set = args[0].strip().lower()
        if deployment_set not in self.valid_deployment_sets():
            raise ValueError

        self._objects = []
        for arg in args[2:]:
            self._objects.append(arg)


        self._deployment_set = deployment_set


    def valid_deployment_sets(self):
        return [
            'stable'
        ]

    def do_deployment(self, deployment_set: str):
        if deployment_set.strip().lower() == 'stable':
            self.deploy_to_stable(self._objects)
        else:
            raise NotImplementedError

    @staticmethod
    def deploy_to_stable(objects: []):
        cfg = DatabaseDeploymentConfig('database_deploymenet_config.json')
        source_db = cfg.get_database('Northwind_Beta')
        destination_db = cfg.get_database('Northwind')
        deployment = MssqlDatabaseDeployment(source_db,
                                             destination_db,
                                             cfg.get_local_repo_path(),
                                             objects,
                                             DatabaseDeploymentConfig.get_logger())
        deployment.deploy()




class DatabaseDeployment(object):
    def __int__(self,
                database_type: DatabaseType):
        self._database_type = database_type

class MssqlDatabaseDeployment(DatabaseDeployment):
    def __init__(self,
                 source_database: MssqlDatabase,
                 destination_database: MssqlDatabase,
                 local_repo_path: str,
                 objects: [],
                 logger: logging.Logger=None):
        self._source_database = source_database,
        self._destination_database = destination_database,

        if type(self._source_database) in [tuple, list]:
            self._source_database = self._source_database[0]

        if type(self._destination_database) in [tuple, list]:
            self._destination_database = self._destination_database[0]

        self._local_repo_path = local_repo_path
        self._objects = objects

        if not logger:
            self._logger = logging.getLogger(__name__)
        else:
            self._logger = logger

        if self._is_have_bad_objects():
            self._logger.error('Objects argument contains one or more objects that are not found')
            raise ValueError

    def deploy(self):
        self._logger.info('Starting deployment process...')

        self._do_hard_reset()
        self.script_objects()
        self._commit_changes()
        self._deploy_changes()

        self._logger.info('Finished deployment')

    def _do_hard_reset(self):
        self._logger.info('Doing hard reset')

        os.chdir(self._local_repo_path)
        args = [
            'git',
            'reset',
            '--hard',
            'HEAD'
        ]
        subprocess.check_output(args, shell=True)

        self._logger.info('Git pull')
        args = [
            'git',
            'pull'
        ]
        subprocess.check_output(args, shell=True)

        self._logger.info('Checkout master')
        args = [
            'git',
            'checkout',
            'master'
        ]
        subprocess.check_output(args, shell=True)


    def _is_have_bad_objects(self):
        bad_objects = self._source_database.get_objects_not_found(self._objects)

        if bad_objects or len(bad_objects) > 0:
            self._logger.warning('Some objects cannot be found in the source database {}'.format(self._source_database.database))
            for obj in bad_objects:
                self._logger.warning('\t{}'.format(obj))
            return True
        else:
            return False

    def script_objects(self):
        self._logger.info('Scripting objects: {}'.format(self._objects))
        self._source_database.script_objects(self._objects, self._destination_database.local_path)

    def get_sql_from_path(self):
        from StringUtil import StringUtil
        sql = []
        for o in os.listdir(os.path.abspath(self._destination_database.local_path)):
            if not '.'.join(o.split('.')[0:2]).strip().lower() in [str(x).strip().lower() for x in self._objects]:
                continue

            sql.append(MssqlDatabase.read_script(os.path.join(self._destination_database.local_path, o)))

        return sql
        # return '\nGO\n'.join(sql)

    def _deploy_changes(self):
        os.chdir(self._destination_database.local_path) # TODO: Probably can take out
        for f in self._get_sql_file_paths():
            if not f:
                continue
            file_path = os.path.join(self._destination_database.local_path, f)
            self._logger.info('Deploying {}'.format(file_path))
            self._destination_database.execute_script(file_path)

    def _get_scripted_objects(self):
        return self._source_database.script_objects(self._objects, None, False)

    def _get_sql_file_paths(self):
        paths = []
        for o in os.listdir(os.path.abspath(self._destination_database.local_path)):
            if not '.'.join(o.split('.')[0:2]).strip().lower() in [str(x).strip().lower() for x in self._objects]:
                continue
            paths.append(os.path.join(self._destination_database.local_path, o))
            # paths.append(o)
        return paths



    def _commit_changes(self):
        from StringUtil import StringUtil

        self._logger.info('Checking Git repo if there is anything to commit')
        args = [
            'git',
            'status'
        ]
        if 'nothing to commit' not in str(subprocess.check_output(args, shell=True)).lower():
            self._logger.info('Changes found. Committing changes...')
            args = [
                'git',
                'add',
                '.',
            ]
            subprocess.check_output(args, shell=True)

            args = [
                'git',
                'commit',
                '-m',
                '"{} - {}"'.format(StringUtil.get_timestamp_full(), ' '.join(self._objects))
            ]
            subprocess.check_output(args, shell=True)

        self._logger.info('Pushing changes to origin master')
        args = [
            'git',
            'push',
            '-u',
            'origin',
            'master'
        ]
        subprocess.check_output(args, shell=True)

    def exec_sql(self, sql):
        self._destination_database.execute_sql(sql)




