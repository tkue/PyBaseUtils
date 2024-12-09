from DatabaseUtils.Database import MssqlDatabase

class SqlObject(object):
    pass


class MssqlObject(SqlObject):
    def __init__(self,
                 object_id: int,
                 name: str,
                 schema: str,
                 last_change_date: str,
                 database: MssqlDatabase):
        self.object_id = object_id
        self.name = name
        self.schema = schema
        self.last_change_date = last_change_date
        self._database = database