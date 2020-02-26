from sqlalchemy import MetaData, Table, String, Column, select, Numeric, exc
from sqlalchemy.sql import text
import csv
from bin import helpers


def hub_mapping_extended(eng):
    ''' join hub mapping and column information, write to csv'''
    connection = eng.connect()
    if eng.dialect.name == 'sqlite':
        attach_db = "attach database './metadata.db' as DV_MTD"
        try:
            connection.execute(attach_db)
        except exc.OperationalError:
            pass

    raw_select_stmt = '''
    SELECT 
        hub_cols.HUB_NAME,
        hub_cols.HUB_BUSINESS_KEY,
        hub_maps.SOURCE_NAME,
        cols.DATA_TYPE,
        cols.CHARACTER_MAXIMUM_LENGTH,
        cols.NUMERIC_PRECISION,
        cols.NUMERIC_SCALE
    FROM 
        DV_MTD.hub_business_keys hub_cols
        JOIN DV_MTD.column_dtypes cols
        ON hub_cols.HUB_BUSINESS_KEY = cols.column_name
        JOIN DV_MTD.hub_mappings hub_maps
        ON hub_cols.hub_name = hub_maps.hub_name
        AND hub_maps.source_name = cols.table_name
    '''
    with open("./input/mappings/hub_mapping_extended.csv", 'w') as outputfile:
        writer = csv.writer(outputfile)
        full_defs = connection.execute(raw_select_stmt)
        writer.writerow(full_defs.keys())
        for full_def in full_defs:
            writer.writerow(full_def)
    

def link_mapping_extended(eng):
    ''' join link and hub mapping and column information, write to csv'''
    connection = eng.connect()
    if eng.dialect.name == 'sqlite':
        attach_db = "attach database './metadata.db' as DV_MTD"
        try:
            connection.execute(attach_db)
        except exc.OperationalError:
            pass

    raw_select_stmt = '''
    SELECT 
        lnk_maps.LINK_NAME,
        lnk_maps.HUB_NAME,
        lnk_maps.KEY,
        cols.DATA_TYPE,
        cols.CHARACTER_MAXIMUM_LENGTH,
        cols.NUMERIC_PRECISION,
        cols.NUMERIC_SCALE
    FROM 
        DV_MTD.LINK_MAPPINGS lnk_maps
        JOIN HUB_MAPPINGS hub_maps
        ON hub_maps.HUB_NAME = lnk_maps.HUB_NAME
        JOIN DV_MTD.COLUMN_DTYPES cols
        ON cols.TABLE_NAME = hub_maps.SOURCE_NAME
        AND cols.COLUMN_NAME = lnk_maps.KEY
    '''
    with open("./input/mappings/link_mapping_extended.csv", 'w') as outputfile:
        writer = csv.writer(outputfile)
        full_defs = connection.execute(raw_select_stmt)
        writer.writerow(full_defs.keys())
        for full_def in full_defs:
            writer.writerow(full_def)
    
def sat_mapping_extended(eng):
    ''' join link and hub mapping and column information, write to csv'''
    connection = eng.connect()
    if eng.dialect.name == 'sqlite':
        attach_db = "attach database './metadata.db' as DV_MTD"
        try:
            connection.execute(attach_db)
        except exc.OperationalError:
            pass

    raw_select_stmt = '''
    SELECT TABLE_NAME AS SOURCE_NAME
        , 'SAT_' || TABLE_NAME AS TARGET_NAME
        , SOURCE_ATTRIBUTE_NAME
        , SOURCE_ATTRIBUTE_NAME_DATATYPE
        , SOURCE_ATTRIBUTE_CHARACTER_MAXIMUM_LENGTH
        , SOURCE_ATTRIBUTE_NUMERIC_PRECISION
        , SOURCE_ATTRIBUTE_NUMERIC_SCALE
        , TARGET_ATTRIBUTE_NAME
        , TARGET_ATTRIBUTE_NAME_DATATYPE
        , TARGET_ATTRIBUTE_CHARACTER_MAXIMUM_LENGTH
        , TARGET_ATTRIBUTE_NUMERIC_PRECISION
        , TARGET_ATTRIBUTE_NUMERIC_SCALE
        lnk_maps.LINK_NAME,
        lnk_maps.HUB_NAME,
        lnk_maps.KEY,
        cols.DATA_TYPE,
        cols.CHARACTER_MAXIMUM_LENGTH,
        cols.NUMERIC_PRECISION,
        cols.NUMERIC_SCALE
    FROM 
        DV_MTD.LINK_MAPPINGS lnk_maps
        JOIN HUB_MAPPINGS hub_maps
        ON hub_maps.HUB_NAME = lnk_maps.HUB_NAME
        JOIN DV_MTD.COLUMN_DTYPES cols
        ON cols.TABLE_NAME = hub_maps.SOURCE_NAME
        AND cols.COLUMN_NAME = lnk_maps.KEY
    '''
    with open("./input/mappings/link_mapping_extended.csv", 'w') as outputfile:
        writer = csv.writer(outputfile)
        full_defs = connection.execute(raw_select_stmt)
        writer.writerow(full_defs.keys())
        for full_def in full_defs:
            writer.writerow(full_def)
    


if __name__ == "__main__":
    # engine = helpers.engine_snowflake('metadata')
    engine_target = helpers.engine_sqlite('metadata')
    # engine_source = helpers.engine_snowflake('source_informationschema')
    # create_metadata_tables(engine_target)
    # fill_metadata_schemas(engine_target)
    # fill_metadata_columns(engine_source, engine_target)
    link_mapping_extended(engine_target)
    engine_target.dispose()
    # engine_source.dispose()

