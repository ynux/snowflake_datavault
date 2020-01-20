from sqlalchemy import create_engine
import configparser

# connect
config = configparser.ConfigParser()
config.read('../conf/config.ini')

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

if __name__ == "__main__":
    connection = eng.connect()
    results = connection.execute('select current_version()').fetchone()
    print(results)
