from sqlalchemy import MetaData, Table, String, Column, select
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
        Column('HUB_NAME', String, nullable=False)
    )

    # no satellite mappings presently, because those are generated automatically
    # one per source table, with no attributes changed or excluded
    # introduce it when changes are needed
    metadata.drop_all(eng)
    metadata.create_all(eng, checkfirst=False)


def fill_metadata_schemas(eng):
    connection = eng.connect()
    metadata = MetaData()
    schemas = Table('SCHEMAS', metadata, autoload=True, autoload_with=eng)
    connection.execute(schemas.delete())
    for role in ['metadata', 'source', 'staging', 'rawvault']:
        if engine.dialect.name == 'sqlite':
            val = {
                'DATABASE': 'None',
                'SCHEMA': role,
                'ROLE': role
            }
        if engine.dialect.name == 'snowflake':
            conn_conf = helpers.read_config(role)
            val = {
                'DATABASE': conn_conf['database'],
                'SCHEMA': conn_conf['schema'],
                'ROLE': role
            }
        connection.execute(schemas.insert().values(val))


def create_metadata_views(eng):
    metadata = MetaData()
    conn = engine.connect()
    # get databases and schemas
    metadata = MetaData()
    schemas = Table('SCHEMAS', metadata, autoload=True, autoload_with=eng)
    get_dbs_schemas = select([schemas])

    dbs_schemas = conn.execute(get_dbs_schemas).fetchall()
    print(dbs_schemas)
    print(type(dbs_schemas))

    create_view = '''
    CREATE VIEW {RV_SCHEMA}.hub_mapping_full (
        HUB_SCHEMA_NAME,
        HUB_NAME,
        HUB_BUSINESS_KEY_DEFINITION,
        SOURCE_SCHEMA_NAME,
        SOURCE_NAME,
        BUSINESS_KEY,
    AS ( SELECT 
            '{RV_SCHEMA}',
            hub_cols.HUB_NAME,
            hub_cols.HUB_BUSINESS_KEY,
            '{SRC_SCHEMA}',
            hub_maps.SOURCE_NAME,
            cols.DATA_TYPE,
            cols.CHARACTER_MAXIMUM_LENGTH,
            cols.NUMERIC_PRECISION,
            cols.NUMERIC_SCALE
        FROM 
            {MTD_SCHEMA}.hub_business_keys hub_cols
            JOIN {SRC_DB}.information_schema.columns cols
            ON hub_cols.hub_name = cols.table_name
            AND hub_cols.HUB_BUSINESS_KEY = cols.column_name
            JOIN {MTD_SCHEMA}.hub_mappings hub_maps
            ON hub_cols.hub_name = hub_maps.hub_name
         WHERE cols.table_schema = '{SRC_SCHEMA}'
    '''.format(
        RV_SCHEMA='DV_RAV',
        SRC_SCHEMA='TPCH_SF1',
        SRC_DB='snowflake_sample_data',
        MTD_SCHEMA='DV_MTD'
    )

    print(create_view)


if __name__ == "__main__":
    # engine = helpers.engine_snowflake('metadata')
    engine = helpers.engine_sqlite('metadata')
    create_metadata_tables(engine)
    fill_metadata_schemas(engine)
    create_metadata_views(engine)
    engine.dispose()

