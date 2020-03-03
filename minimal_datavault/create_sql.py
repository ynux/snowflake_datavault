
from sqlalchemy import Table, MetaData, select
from generate_code import helpers, generate_create_stg_tables


dialect = helpers.read_config('db_dialect')['dialect']
if dialect == 'snowflake':
    engine = helpers.engine_snowflake('metadata')
else:
    engine = helpers.engine_sqlite('metadata')
connection = engine.connect()
metadata = MetaData()
column_dtypes = Table('COLUMN_DTYPES', metadata, autoload=True, autoload_with=engine)
select_column_dtypes = select([column_dtypes])
column_dtypes_results = connection.execute(select_column_dtypes).fetchall()
generate_create_stg_tables.write_create_staging_tables(column_dtypes_results)


