from ConfigUtil import ConfigSingleton
from DatabaseUtils.Database import MssqlDatabase

import pandas

class PandasEtlConfig(ConfigSingleton):
    db_server = 'localhost'
    db_name = 'EtlTesting'
    db_username = 'sa'
    db_password = ''
