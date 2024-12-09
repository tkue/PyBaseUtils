class Connection(object):
    pass


class MssqlConnection(Connection):
    def __init__(self,
                 host: str,
                 database: str,
                 username: str,
                 password: str,
                 port: int = 1433,
                 ):
        self.host = host
        self.database = database
        self.username = username
        self.password = password
        self.port = port
