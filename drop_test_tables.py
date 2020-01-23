from sqlalchemy import create_engine, MetaData, Table, exc
import configparser

# connect
config = configparser.ConfigParser()
config.read('./conf/config.ini')

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


def drop_test_tables(eng, table_list):
    metadata2 = MetaData()
    for table_to_drop in table_list:
        try:
            table_to_drop = Table(table_to_drop, metadata2, autoload=True, autoload_with=eng)
            table_to_drop.drop(eng)
        except exc.NoSuchTableError:
            continue


if __name__ == "__main__":
    drop_test_tables(engine, ['CUSTOMER','LINEITEM','NATION','ORDERS','PART','PARTSUPP','REGION','SUPPLIER', 'SEQUENCETABLE'])
    engine.dispose()
