from sqlalchemy import create_engine
from helpers import read_config

def engine_snowflake(target):
    connection_params = read_config.read_config(target)
 
    engine = create_engine(
        'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'.format(
            user=connection_params['user'],
            password=connection_params['password'],
            account=connection_params['account'],
            database=connection_params['database'],
            schema=connection_params['schema'],
            warehouse=connection_params['warehouse']
        )
    )
    return engine


if __name__ == "__main__":
    # comes back with 4.2.1 and many warnings for me in Jan 2020
    engine = engine_snowflake('source')
    try:
        connection = engine.connect()
        results = connection.execute('select current_version()').fetchone()
        print(results[0])
    finally:
        connection.close()
        engine.dispose()
