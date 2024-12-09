from enum import Enum


class DatabaseType(Enum):
    GENERIC = 'generic'
    SQLITE3 = 'sqlite3'
    MSSQL = 'mssql'
