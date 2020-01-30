import configparser
import os
from sqlalchemy import create_engine, schema, exc


def create_playground_schemas():
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_dir = os.path.join(parent_dir, 'conf')
    configfile = os.path.join(config_dir, "config.ini")
    config = configparser.ConfigParser()
    config.read(configfile)

    engine = create_engine(
        'snowflake://{user}:{password}@{account}/{database}?warehouse={warehouse}'.format(
            user=config['snowflake_credentials']['user'],
            password=config['snowflake_credentials']['password'],
            account=config['snowflake_connection']['account'],
            database=config['playground']['database'],
            warehouse=config['snowflake_connection']['warehouse']
        )
    )

    for playground_schema in ['staging_schema', 'rawvault_schema', 'metadata_schema']:
        try:
            engine.execute(schema.CreateSchema(playground_schema))
        except exc.ProgrammingError:
            pass

    engine.dispose()


if __name__ == "__main__":
    create_playground_schemas()
    