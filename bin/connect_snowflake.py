import configparser
import os
from sqlalchemy import create_engine

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_dir = os.path.join(parent_dir, 'conf')
configfile = os.path.join(config_dir, "config.ini")


def engine_snowflake(configfile):
    config = configparser.ConfigParser()
    config.read(configfile)

    engine = create_engine(
        'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'.format(
            user=config['db_credentials']['user'],
            password=config['db_credentials']['password'],
            account=config['db_connection']['account'],
            database=config['db_connection']['database'],
            schema=config['db_connection']['schema'],
            warehouse=config['db_connection']['warehouse']
        )
    )
    return engine


if __name__ == "__main__":
    # comes back with 3.56.4 and many warnings for me
    engine = engine_snowflake(configfile)
    try:
        connection = engine.connect()
        results = connection.execute('select current_version()').fetchone()
        print(results[0])
    finally:
        connection.close()
        engine.dispose()
