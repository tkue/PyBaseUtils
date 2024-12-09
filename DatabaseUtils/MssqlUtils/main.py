from DatabaseUtils.MssqlUtils import Config
import logging

# from mssql_objects.MssqlObjects import Column
from DatabaseUtils.MssqlUtils.mssql_objects.MssqlObjects import *
from DatabaseUtils.MssqlUtils.Database import DatabaseBackupCollection
import argparse



def do_database_backup():
    for db_backup in Config.get_database_backups(logger=Config.get_logger()):
        print(db_backup.get_changes_from_last_run())
        db_backup.do_full_backup(is_changed_objects_only=False)

def get_arguments(args):
    parser = argparse.ArgumentParser(description='DatabaseUtils arguments')
    parser.add_argument('-s', '--server', help='Database host/server')
    options = parser.parse_args(args)
    return options

if __name__ == '__main__':
