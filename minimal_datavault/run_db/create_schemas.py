from sqlalchemy import create_engine, schema, exc
from generate_code import helpers


def create_playground_schemas():
    dialect = helpers.read_config('db_dialect')['dialect']
    if dialect != 'snowflake':
        pass
    else:
        connection_params = helpers.read_config('metadata')
        engine = create_engine(
            'snowflake://{user}:{password}@{account}/{database}?warehouse={warehouse}'.format(
                user=connection_params['user'],
                password=connection_params['password'],
                account=connection_params['account'],
                database=connection_params['database'],
                warehouse=connection_params['warehouse']
            )
        )
   
        metadata_schema = connection_params['schema']
        rawvault_schema = helpers.read_config('rawvault')['schema']
        staging_schema = helpers.read_config('staging')['schema']
   
        for playground_schema in [metadata_schema, rawvault_schema, staging_schema]:
            try:
                engine.execute(schema.CreateSchema(playground_schema))
            except exc.ProgrammingError:
                pass
   
        engine.dispose()


if __name__ == "__main__":
    create_playground_schemas()
    
