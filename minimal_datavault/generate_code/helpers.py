import configparser
import os
from sqlalchemy import create_engine

# from generate_code import config_dir


def read_config(target: str, config_ini='config.ini'):
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_dir = os.path.join(parent_dir, 'conf')
    #config_dir = "/Users/skim/Documents/eon/synthetic_data_vault/minimal_datavault/conf"
    configfile = os.path.join(config_dir, config_ini)

    config = configparser.ConfigParser()
    config.read(configfile)

    if target == 'db_dialect':
        dialect = config['db']['dialect']
    elif target == 'source':
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
    elif target == 'source_informationschema':
        database = config['databases_schemas']['source_database']
        schema = 'information_schema'
    else:
        raise ValueError("unknown target {}".format(target))

    user = config['snowflake_credentials']['user']
    password = config['snowflake_credentials']['password']
    account = config['snowflake_connection']['account']
    warehouse = config['snowflake_connection']['warehouse']

    if target == 'db_dialect':
        config_dict = {
            "dialect": dialect
        }
    else: 
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


def engine_sqlite(target):
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sqlite_db_dir = os.path.join(parent_dir, 'sqlite_dbs')
    engine = create_engine(
        'sqlite:////{sqlite_db_dir}/{target}.db'.format(
            sqlite_db_dir=sqlite_db_dir,
            target=target
        )
    )
    return engine


if __name__ == "__main__":
    config_dir = "/Users/skim/Documents/eon/synthetic_data_vault/minimal_datavault/conf"
    configfile = os.path.join(config_dir, "config.ini.sample")
    dialect = read_config('db_dialect')['dialect']
    if dialect == 'snowflake':
        configfile = os.path.join(config_dir, "config.ini")
        engine = engine_snowflake('source')
        try:
            connection = engine.connect()
            results = connection.execute('select current_version()').fetchone()
            print(results[0])
            # comes back with 4.6.1 and many warnings for me in Feb 2020
        finally:
            connection.close()
            engine.dispose()
    if dialect == 'sqlite':
        engine = engine_sqlite('source')
        print(engine.dialect.name)
        try:
            connection = engine.connect()
            results = connection.execute('select sqlite_version();').fetchone()
            print(results[0])
            # comes back with 3.28.0 for me in Jan 2020
        finally:
            connection.close()
            engine.dispose()
            # you might want to remove the empty source.db file afterwards

    
