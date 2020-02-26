from sqlalchemy import MetaData, Table, String, Column, select, Numeric
from sqlalchemy import create_engine
from sqlalchemy.engine import reflection
from sqlalchemy.sql import text
from bin import helpers


def create_metadata_tables(eng):
    ''' drop and create metadata tables for datavault creation '''
    metadata = MetaData()

    schemas = Table('SCHEMAS', metadata,
        Column('SCHEMA', String, nullable=False, primary_key=True),
        Column('DATABASE', String, nullable=False),
        Column('ROLE', String, nullable=True)
    )

    hub_mappings = Table('HUB_MAPPINGS', metadata,
        Column('HUB_NAME', String, nullable=False, primary_key=True),
        Column('SOURCE_NAME', String, nullable=False)
    )

    hub_business_keys = Table('HUB_BUSINESS_KEYS', metadata,
        Column('HUB_BUSINESS_KEY', String, nullable=False, primary_key=True),
        Column('HUB_NAME', String, nullable=False)
    )

    link_mappings = Table('LINK_MAPPINGS', metadata,
        Column('LINK_NAME', String, nullable=False),
        Column('HUB_NAME', String, nullable=False),
        Column('KEY', String, nullable=False)
    )

    # no satellite mappings presently, because those are generated automatically
    # one per source table, with no attributes changed or excluded
    # introduce it when changes are needed
    column_dtypes = Table('COLUMN_DTYPES', metadata,
        Column('TABLE_NAME', String, nullable=True),
        Column('COLUMN_NAME', String, nullable=True),
        Column('IS_NULLABLE', String(3), nullable=True),
        Column('DATA_TYPE', String, nullable=True),
        Column('CHARACTER_MAXIMUM_LENGTH', Numeric(38), nullable=True),
        Column('NUMERIC_PRECISION', Numeric(38), nullable=True),
        Column('NUMERIC_SCALE', Numeric(38), nullable=True)
    )

    metadata.drop_all(eng)
    metadata.create_all(eng, checkfirst=False)


def fill_metadata_schemas(eng):
    connection = eng.connect()
    metadata = MetaData()
    schemas = Table('SCHEMAS', metadata, autoload=True, autoload_with=eng)
    insp = reflection.Inspector.from_engine(engine)
    connection.execute(schemas.delete())
    for role in ['metadata', 'source', 'staging', 'rawvault']:
        conn_conf = helpers.read_config(role)
        if eng.dialect.name == 'sqlite':
            val = {
                'DATABASE': 'None',
                'SCHEMA': conn_conf['schema'],
                'ROLE': role
            }
        if eng.dialect.name == 'snowflake':
            val = {
                'DATABASE': conn_conf['database'],
                'SCHEMA': conn_conf['schema'],
                'ROLE': role
            }
        connection.execute(schemas.insert().values(val))


def fill_metadata_columns(eng_src, eng_tgt):
    connection_tgt = eng_tgt.connect()
    connection_src = eng_src.connect()
    metadata2 = MetaData()
    columns_dtypes = Table('COLUMN_DTYPES', metadata2, autoload=True, autoload_with=eng_tgt)
    # sqlalchemy table reflection for sample db information schema is broken
    query = '''select col.TABLE_NAME
        , col.COLUMN_NAME
        , col.IS_NULLABLE
        , col.DATA_TYPE
        , col.CHARACTER_MAXIMUM_LENGTH
        , col.NUMERIC_PRECISION
        , col.NUMERIC_SCALE
        FROM information_schema.columns col
        JOIN information_schema.tables tab
        ON col.table_schema = tab.table_schema
        AND col.table_name = tab.table_name
        WHERE col.table_schema = 'TPCH_SF1'
        AND tab.table_type = 'BASE TABLE'
    '''
    values = connection_src.execute(query)
    connection_tgt.execute(columns_dtypes.delete())
    for val in values:
        valdict = {}
        valdict['TABLE_NAME'] = val[0]
        valdict['COLUMN_NAME'] = val[1]
        valdict['IS_NULLABLE'] = val[2]
        valdict['DATA_TYPE'] = val[3]
        valdict['CHARACTER_MAXIMUM_LENGTH'] = val[4]
        valdict['NUMERIC_PRECISION'] = val[5]
        valdict['NUMERIC_SCALE'] = val[6]
        connection_tgt.execute(columns_dtypes.insert().values(valdict))



if __name__ == "__main__":
    dialect = helpers.read_config('db_dialect')['dialect']
    if dialect == 'snowflake':
        engine_target = helpers.engine_snowflake('metadata')
    else:
        engine_target = helpers.engine_sqlite('metadata')
    engine_source = helpers.engine_snowflake('source_informationschema')
    #create_metadata_tables(engine_target)
    fill_metadata_schemas(engine_target)
    #fill_metadata_columns(engine_source, engine_target)
    engine_target.dispose()
    engine_source.dispose()

