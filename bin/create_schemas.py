import configparser
from sqlalchemy import create_engine, schema, exc
from helpers import read_config

def create_playground_schemas():

    connection_params = read_config.read_config('metadata')

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
    rawvault_schema = read_config.read_config('rawvault')['schema']
    staging_schema = read_config.read_config('staging')['schema']

    for playground_schema in [metadata_schema, rawvault_schema, staging_schema]:
        try:
            engine.execute(schema.CreateSchema(playground_schema))
        except exc.ProgrammingError:
            pass

    engine.dispose()


if __name__ == "__main__":
    create_playground_schemas()
    