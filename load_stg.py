
from sqlalchemy import MetaData, Table, select


def insert_into_stage(table_name, eng_src, eng_tgt, num_rows):
    metadata = MetaData()
    table = Table(table_name, metadata, autoload=True, autoload_with=eng_src)
    conn_src = engine_source.connect()
    conn_tgt = engine_target.connect()

    select_stmt = select([table]).limit(num_rows)
    results = conn_src.execute(select_stmt)

    conn_tgt.execute(table.delete())
    for result in results:
        insert_stmt = table.insert(result)
        conn_tgt.execute(insert_stmt)


if __name__ == "__main__":
    from bin import connect_snowflake
    import os
    from sqlalchemy import create_engine, inspect
    basedir = os.path.dirname(os.path.abspath(__file__))
    configfile = os.path.join(basedir, 'conf', 'config.ini')
    engine_source = connect_snowflake.engine_snowflake(configfile, 'source_db_schema')
    engine_target = connect_snowflake.engine_snowflake(configfile, 'staging_db_schema')
    # engine_source = create_engine('sqlite:///sqlite3_src.db')
    # engine_target = create_engine('sqlite:///sqlite3_tgt.db')
    #tables = ['CUSTOMER']
    inspector = inspect(engine_target)
    tables = inspector.get_table_names()
    for table in tables:
        insert_into_stage(table, engine_source, create_engine, 10)

    engine_source.dispose()
    engine_target.dispose()

