from enum import Enum


# TODO: Is this all the defined types?
class MssqlScripterObjectType(Enum):
    USER_DEFINED_FUNCTION = 'UserDefinedFunction'
    STORED_PROCEDURE = 'StoredProcedure'
    VIEW = 'View'
    TABLE = 'Table'
    DATABASE = 'Database'
    SCHEMA = 'Schema'
    EXTENDED_PROPERTY = 'ExtendedProperty'
    FULL_TEXT_CATALOG = 'FullTextCatalog'
    XML_SCHEMA_COLLECTION = 'XmlSchemaCollection'
    USER_DEFINED_DATA_TYPE = 'UserDefinedDataType'
    DDL_TRIGGER = 'UserDefinedDataType'
