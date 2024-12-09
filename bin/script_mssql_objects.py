import logging

from DatabaseUtils.Database import MssqlDatabase
from DatabaseUtils.MssqlUtils.DatabaseBackup import MssqlDatabaseBackup

cfg_file = 'C:\\Users\\tomku\\Dropbox\\bin\\BaseUtils\\DatabaseUtils\\config_database.json'
tmp_path = 'C:\\Users\\tomku\\Desktop\\temp'

logger = logging.getLogger(__name__)

db = MssqlDatabase('localhost', 'northwind', 'sa', 'password', None, tmp_path)
bak = MssqlDatabaseBackup(tmp_path, db, logger)
bak.get_script_objects(['dbo.CustOrdersOrders', 'Reporting.GetOrderDetails', 'test'], tmp_path)
