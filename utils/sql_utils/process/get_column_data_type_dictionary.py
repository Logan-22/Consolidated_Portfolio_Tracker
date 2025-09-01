from utils.sql_utils.process.fetch_queries import fetch_queries_as_dictionaries

def get_column_type_dictionary(schema_name, table_name):
    column_type_data = {}
    column_type_data_list = fetch_queries_as_dictionaries(f"""
SELECT 
    COLUMN_NAME
    ,DATA_TYPE
    ,NUMERIC_PRECISION
    ,NUMERIC_SCALE
FROM
    INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_SCHEMA   = '{schema_name}'
    AND TABLE_NAME = '{table_name}'
ORDER BY ORDINAL_POSITION;
""", 'return_none', fetch = 'All')
    for column in column_type_data_list:
        if 'char' in column['DATA_TYPE'] or 'text' in column['DATA_TYPE'] or 'string' in column['DATA_TYPE']:
            column_type_data[column['COLUMN_NAME']] = {'type':'STRING'}
        elif 'date' in column['DATA_TYPE'] or 'time' in column['DATA_TYPE']:
            column_type_data[column['COLUMN_NAME']] = {'type': 'DATE'}
        elif 'int' in column['DATA_TYPE'] or 'decimal' in column['DATA_TYPE'] or 'numeric' in column['DATA_TYPE']:
            column_type_data[column['COLUMN_NAME']] = {'type' : 'NUMBER', 'precision' : column['NUMERIC_PRECISION'], 'scale' : column['NUMERIC_SCALE']}
        else:
            column_type_data[column['COLUMN_NAME']] = {'type':'STRING'}
    return column_type_data