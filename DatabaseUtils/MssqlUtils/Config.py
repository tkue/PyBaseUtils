from DatabaseUtils.MssqlUtils.Connection import MssqlConnection
from DatabaseUtils.Database import MssqlDatabase
from DatabaseUtils.MssqlUtils import DatabaseBackup

import logging

__LOGGER = logging.getLogger(__name__)
__LOGGER.level = logging.INFO

__PATH = '.\\mssqlutils_temp'
__LOG_NAME = 'mssqlutils.log'

__USERNAME = 'sa'
__PASSWORD = ''

__DATABASES = [
    {
        'host': 'localhost',
        'database': 'Northwind',
        'username': '',
        'password': ''
    }
    # , {
    #     'host': '172.17.0.1',
    #     'database': 'pubs',
    #     'username': '',
    #     'password': ''
    # }, {
    #     'host': '172.17.0.1',
    #     'database': 'AdventureWorksDW2014',
    #     'username': '',
    #     'password': ''
    # }, {
    #     'host': '172.17.0.1',
    #     'database': 'AdventureWorks2014',
    #     'username': '',
    #     'password': ''
    # }, {
    #     'host': '172.17.0.1',
    #     'database': 'AdventureWorks2016CTP3',
    #     'username': '',
    #     'password': ''
    # }, {
    #     'host': '172.17.0.1',
    #     'database': 'WideWorldImporters',
    #     'username': '',
    #     'password': ''
    # }
]

def get_log_filename():
    return __LOG_NAME

def get_connections():
    username = ''
    password = ''

    connections = []

    for database in __DATABASES:
        if not database['username']:
            username = __USERNAME
        else:
            username = database['username']

        if not database['password']:
            password = __PASSWORD
        else:
            password = database['password']

        conn = MssqlConnection(host=database['host'],
                               database=database['database'],
                               username=username,
                               password=password)

        connections.append(conn)

    return connections


def get_databases(db_name: str=None):
    databases = []

    for conn in get_connections():
        if db_name:
            if conn.host.strip().lower() != db_name.strip().lower():
                continue
        databases.append(MssqlDatabase(connection=conn))

    return databases


def get_database_backups(logger: logging.Logger = None):
    db_backups = []

    if not logger:
        logger = __LOGGER

    for db in get_databases():
        db_backups.append(DatabaseBackup.MssqlDatabaseBackup(path=__PATH,
                                              database=db,
                                              logger=logger))

    return db_backups

def get_logger():
    logger = logging.getLogger('MssqlUtils')
    logger.setLevel(logging.INFO)

    fh = logging.FileHandler(get_log_filename())
    fh.setLevel(logging.INFO)

    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger