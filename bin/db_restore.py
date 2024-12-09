from DatabaseUtils import Database
from StringUtil import StringUtil
import os

db1 = 'Northwind_Beta'
db2 = 'Northwind'
path = '.'

db = Database.MssqlDatabase(server='localhost', database='Northwind', username='tom', password='password')
rows = db.compare_objects_in_databases(db1, db2)

to_create = []
to_modify = []




for row in rows:
    compare_type = row[0].lower()
    if compare_type == 'not exists':
        to_create.append(row[3])

    if  compare_type == 'hash':
        to_modify.append(row[3])



db = Database.MssqlDatabase(server='172.17.0.1', database=db1, username='sa', password='password')

if to_create and len(to_create) > 0:
    script = db.script_objects(to_create)

if to_modify and len(to_modify) > 0:
    script = db.script_objects(to_modify)

sql = """SELECT ObjectId = OBJECT_ID('dbo.Test2')
                ;WITH c AS (
                    SELECT 
                        CONCAT(s.name, '.', o.name) AS FullName 
                    FROM sys.objects o
                    JOIN sys.schemas s ON o.schema_id = s.schema_id
                )

                SELECT ISNULL(
                        (SELECT TOP 1 1
                        FROM c 
                        WHERE 
                            REPLACE(REPLACE('{}', '[', ''), ']', '') = c.FullName)
                , 0) AS IsExists """.format('dbo.Test')
print(db.get_rows_from_sql(sql))
print(script)



