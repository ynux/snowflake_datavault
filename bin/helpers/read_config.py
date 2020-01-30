import configparser
import os

grandparent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
config_dir = os.path.join(grandparent_dir, 'conf')
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
 

if __name__ == "__main__":
    configfile = os.path.join(config_dir, "config.ini.sample")
    print(read_config('source'))
    # {'user': 'XXX', 'password': 'XXX', 'account': 'XXX.xxxregion', 
    # 'database': 'snowflake_sample_data', 'schema': 'TPCH_SF1', 'warehouse': 'xxx'}
