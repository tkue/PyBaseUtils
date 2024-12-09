from DatabaseUtils.Database import MssqlDatabase

_databases = [
    [{
        'name': 'Northwind',
        'rel_path': '..\\Databases\\Northwind',
        'server': 'localhost',
        'username': 'sa',
        'password': ''
    }],
    [{
        'name': 'WideWorldImporters',
        'rel_path': '..\\Databases\\Northwind',
        'server': 'localhost',
        'username': 'sa',
        'password': ''
    }]
]


def get_databases_as_dict(database_name: str):
    if not database_name:
        return _databases
    else:
        for db in _databases:
            if dict(db[0])['name'].strip().lower() == database_name.strip().lower():
                return db


def get_databases_as_obj(database_name: str):
    databases = []
    for db in get_databases_as_dict(database_name):
        databases.append(MssqlDatabase(server=db['server'],
                                       database=db['name'],
                                       username=db['username'],
                                       password=db['password'],
                                       local_path=db['rel_path'],
                                       port=None))

    return databases


if __name__ == '__main__':
    for db in _databases:
        print(db[0]['name'])
