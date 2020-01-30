import configparser
import os
from sqlalchemy import create_engine

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_dir = os.path.join(parent_dir, 'conf')
configfile = os.path.join(config_dir, "config.ini")


def engine_snowflake(configfile, target):
    config = configparser.ConfigParser()
    config.read(configfile)

    if target == 'source':
        database = config['source']['database'],
        schema = config['source']['schema']
    elif target == 'staging':
        database = config['playground']['database'],
        schema = config['playground']['staging_schema']
    elif target == 'rawvault':
        database = config['playground']['database'],
        schema = config['playground']['rawvault_schema']
    elif target == 'metadata':
        database = config['playground']['database'],
        schema = config['playground']['metadata_schema']
    elif target == 'noschema':
        database = config['playground']['database'],
        schema = ''
 
    engine = create_engine(
        'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'.format(
            user=config['snowflake_credentials']['user'],
            password=config['snowflake_credentials']['password'],
            account=config['snowflake_connection']['account'],
            database=database,
            schema=schema,
            warehouse=config['snowflake_connection']['warehouse']
        )
    )
    return engine


if __name__ == "__main__":
    # comes back with 4.2.1 and many warnings for me in Jan 2020
    engine = engine_snowflake(configfile, 'source')
    try:
        connection = engine.connect()
        results = connection.execute('select current_version()').fetchone()
        print(results[0])
    finally:
        connection.close()
        engine.dispose()
