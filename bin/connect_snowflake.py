import configparser
import os
from sqlalchemy import create_engine

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_dir = os.path.join(parent_dir, 'conf')
configfile = os.path.join(config_dir, "config.ini")


def engine_snowflake(configfile, target):
    config = configparser.ConfigParser()
    config.read(configfile)

    # target is e.g. source_db_schema, stage_db_schema, rawvault_db_schema
    engine = create_engine(
        'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'.format(
            user=config['snowflake_credentials']['user'],
            password=config['snowflake_credentials']['password'],
            account=config['snowflake_connection']['account'],
            database=config[target]['database'],
            schema=config[target]['schema'],
            warehouse=config['snowflake_connection']['warehouse']
        )
    )
    return engine


if __name__ == "__main__":
    # comes back with 3.56.4 and many warnings for me
    engine = engine_snowflake(configfile, 'source_db_schema')
    try:
        connection = engine.connect()
        results = connection.execute('select current_version()').fetchone()
        print(results[0])
    finally:
        connection.close()
        engine.dispose()
