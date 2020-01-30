import configparser
import os
from sqlalchemy import create_engine

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_dir = os.path.join(parent_dir, 'conf')
configfile = os.path.join(config_dir, "config.ini")


def read_config(target: str):
    config = configparser.ConfigParser()
    config.read(configfile)

    if target == 'source':
        database = config['databases_schemas']['source_database']
        schema = config['databases_schemas']['source_schema']
    elif target == 'staging':
        database = config['databases_schemas']['playground_database']
        schema = config['databases_schemas']['staging_schema']
    elif target == 'rawvault':
        database = config['databases_schemas']['playground_database']
        schema = config['databases_schemas']['rawvault_schema']
    elif target == 'metadata':
        database = config['databases_schemas']['playground_database']
        schema = config['databases_schemas']['metadata_schema']

    user = config['snowflake_credentials']['user']
    password = config['snowflake_credentials']['password']
    account = config['snowflake_connection']['account']
    warehouse = config['snowflake_connection']['warehouse']

    config_dict = {
        "user": user,
        "password": password,
        "account": account,
        "database": database,
        "schema": schema,
        "warehouse": warehouse
        }
    return config_dict


def engine_snowflake(target):
    connection_params = read_config(target)
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
    configfile = os.path.join(config_dir, "config.ini.sample")
    print(read_config('source'))
    # {'user': 'XXX', 'password': 'XXX', 'account': 'XXX.xxxregion', 
    # 'database': 'snowflake_sample_data', 'schema': 'TPCH_SF1', 'warehouse': 'xxx'}

    # comes back with 4.2.1 and many warnings for me in Jan 2020
    configfile = os.path.join(config_dir, "config.ini")
    print(read_config('source'))
    engine = engine_snowflake('source')
    try:
        connection = engine.connect()
        results = connection.execute('select current_version()').fetchone()
        print(results[0])
    finally:
        connection.close()
        engine.dispose()
 
