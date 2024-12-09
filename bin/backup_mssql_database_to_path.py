import logging
from DatabaseUtils.MssqlUtils.DatabaseBackup import MssqlDatabaseBackup
from DatabaseUtils.Database import MssqlDatabase

local_path = 'C:\\Users\\tomku\\Dropbox\\bin\\mssqldatabases\\Northwind'

if __name__ == '__main__':
    db = MssqlDatabase('localhost', 'Northwind', 'sa', 'password', None, local_path)

    logger = logging.getLogger(__name__)

    backup = MssqlDatabaseBackup(local_path, db, logger)
    backup.do_full_backup(False, True)

